import asyncio
from datetime import datetime, timezone
import time
from typing import Any, List, Optional
from pydantic import ValidationError

from pystreampulse.models import (
    DeadLetterEvent,
    PipelineMetrics,
    ProcessedTelemetryEvent,
    RawTelemetryPacket,
)
from pystreampulse.processor import TelemetryProcessor
from pystreampulse.sinks import BaseSink


class AsyncStreamPipeline:
    """
    Motor de streaming asíncrono de alto rendimiento con:
    - Control de contrapresión (Backpressure) mediante cola acotada.
    - Dead-Letter Queue (DLQ) para tolerancia a fallos y paquetes corruptos.
    - Pool de workers concurrentes asíncronos.
    - Sinks múltiples con vaciado por lotes y timeout.
    """

    def __init__(
        self,
        queue_capacity: int = 10_000,
        num_workers: int = 4,
        sinks: Optional[List[BaseSink]] = None,
        processor: Optional[TelemetryProcessor] = None,
        batch_size: int = 100,
        flush_interval_sec: float = 1.0,
    ):
        self.queue_capacity = queue_capacity
        self.num_workers = num_workers
        self.sinks = sinks or []
        self.processor = processor or TelemetryProcessor()
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec

        self._input_queue: asyncio.Queue[Optional[RawTelemetryPacket]] = asyncio.Queue(maxsize=queue_capacity)
        self._dlq: asyncio.Queue[DeadLetterEvent] = asyncio.Queue()
        self._dlq_records: List[DeadLetterEvent] = []

        self._workers: List[asyncio.Task] = []
        self._flusher_task: Optional[asyncio.Task] = None
        self._is_running = False

        # Métricas de telemetría interna
        self._start_time: Optional[float] = None
        self._total_ingested = 0
        self._total_processed = 0
        self._total_dropped = 0
        self._total_anomalies = 0
        self._latency_sum_ms = 0.0

    async def start(self) -> None:
        """Inicia el pool de workers y la tarea de descarga periódica de sinks."""
        if self._is_running:
            return

        self._is_running = True
        self._start_time = time.perf_counter()

        # Iniciar workers concurrentes
        for worker_id in range(self.num_workers):
            task = asyncio.create_task(self._worker_loop(worker_id))
            self._workers.append(task)

        # Iniciar tarea de autoflush periódico
        self._flusher_task = asyncio.create_task(self._periodic_flush_loop())

    async def submit_raw(self, payload: Any, timeout: float = 1.0) -> bool:
        """
        Ingesta un mensaje sin procesar (dict, JSON string o modelo).
        Si la carga es inválida, se deriva automáticamente a la DLQ.
        Aplica contrapresión si la cola está al 100% de capacidad.
        """
        self._total_ingested += 1

        try:
            if isinstance(payload, RawTelemetryPacket):
                packet = payload
            elif isinstance(payload, dict):
                packet = RawTelemetryPacket(**payload)
            elif isinstance(payload, str):
                packet = RawTelemetryPacket.model_validate_json(payload)
            else:
                raise ValueError(f"Tipo de carga no admitida: {type(payload)}")

            # Encolar con timeout para evitar bloqueo indefinido en picos
            await asyncio.wait_for(self._input_queue.put(packet), timeout=timeout)
            return True

        except (ValidationError, ValueError, TypeError) as err:
            self._total_dropped += 1
            dlq_event = DeadLetterEvent(
                raw_payload=payload,
                error_reason=f"Validación fallida: {str(err)}"
            )
            self._dlq_records.append(dlq_event)
            await self._dlq.put(dlq_event)
            return False
        except asyncio.TimeoutError:
            self._total_dropped += 1
            dlq_event = DeadLetterEvent(
                raw_payload=payload,
                error_reason="Backpressure timeout: Cola de ingesta saturada"
            )
            self._dlq_records.append(dlq_event)
            await self._dlq.put(dlq_event)
            return False

    async def _worker_loop(self, worker_id: int) -> None:
        """Bucle de consumo y procesamiento de cada worker."""
        batch: List[ProcessedTelemetryEvent] = []

        while self._is_running:
            try:
                packet = await self._input_queue.get()
                if packet is None:
                    # Señal de apagado (Poison pill)
                    self._input_queue.task_done()
                    break

                # Procesamiento y cálculo de anomalías
                event = self.processor.process(packet)
                self._total_processed += 1
                self._latency_sum_ms += event.latency_ms
                if event.is_anomalous:
                    self._total_anomalies += 1

                batch.append(event)
                self._input_queue.task_done()

                # Despacho en lote si alcanza batch_size
                if len(batch) >= self.batch_size:
                    await self._dispatch_batch(batch)
                    batch.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._total_dropped += 1

        # Vaciar remanente del worker al terminar
        if batch:
            await self._dispatch_batch(batch)

    async def _dispatch_batch(self, batch: List[ProcessedTelemetryEvent]) -> None:
        """Despacha un lote de eventos a todos los sinks registrados."""
        if not batch or not self.sinks:
            return

        for sink in self.sinks:
            try:
                await sink.write(batch)
            except Exception as e:
                # Fallback de sink
                pass

    async def _periodic_flush_loop(self) -> None:
        """Vacia periódicamente los buffers de los sinks para garantizar baja latencia."""
        while self._is_running:
            try:
                await asyncio.sleep(self.flush_interval_sec)
                await self.flush()
            except asyncio.CancelledError:
                break

    async def flush(self) -> None:
        """Fuerza la escritura a disco/almacén de todos los sinks."""
        for sink in self.sinks:
            try:
                await sink.flush()
            except Exception:
                pass

    async def stop(self) -> None:
        """Apagado ordenado (Graceful Shutdown) vaciando todas las colas pendientes."""
        if not self._is_running:
            return

        # Enviar poison pills para que los workers salgan ordenadamente
        for _ in range(self.num_workers):
            await self._input_queue.put(None)

        # Esperar a que la cola se procese completamente
        await self._input_queue.join()

        self._is_running = False

        if self._flusher_task:
            self._flusher_task.cancel()
            try:
                await self._flusher_task
            except asyncio.CancelledError:
                pass

        # Esperar que finalicen todos los workers
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()

        # Flusher final y cierre de sinks
        for sink in self.sinks:
            await sink.close()

    def get_metrics(self) -> PipelineMetrics:
        """Calcula y reporta las métricas operacionales del pipeline."""
        now = time.perf_counter()
        elapsed = (now - self._start_time) if self._start_time else 1.0
        elapsed = max(elapsed, 0.001)

        avg_lat = (self._latency_sum_ms / self._total_processed) if self._total_processed > 0 else 0.0
        eps = self._total_processed / elapsed
        queue_ratio = self._input_queue.qsize() / max(self.queue_capacity, 1)

        return PipelineMetrics(
            events_ingested=self._total_ingested,
            events_processed=self._total_processed,
            events_dropped_to_dlq=self._total_dropped,
            anomalies_detected=self._total_anomalies,
            avg_latency_ms=round(avg_lat, 3),
            throughput_eps=round(eps, 1),
            queue_backpressure_ratio=round(queue_ratio, 3)
        )

    @property
    def dlq_records(self) -> List[DeadLetterEvent]:
        return self._dlq_records
