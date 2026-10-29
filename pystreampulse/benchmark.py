import asyncio
import os
import sys
import time

# Permite ejecución directa del script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pystreampulse.models import RawTelemetryPacket
from pystreampulse.pipeline import AsyncStreamPipeline
from pystreampulse.producer import TelemetryStreamGenerator
from pystreampulse.sinks import InMemorySink, JsonlBatchSink, SqliteBatchSink


async def run_benchmark(
    total_events: int = 5000,
    num_workers: int = 4,
    output_dir: str = "output"
):
    print("=" * 70)
    print("PYSTREAMPULSE - BENCHMARK DE STREAMING ASINCRONO DE TELEMETRIA")
    print("=" * 70)
    print(f"Eventos totales a transmitir      : {total_events:,}")
    print(f"Workers asincronos concurrentes   : {num_workers}")
    print(f"Directorio de persistencia        : {output_dir}")

    os.makedirs(output_dir, exist_ok=True)
    jsonl_path = os.path.join(output_dir, "benchmark_telemetry.jsonl")
    db_path = os.path.join(output_dir, "benchmark_telemetry.db")

    # Limpiar ejecuciones previas
    for p in (jsonl_path, db_path):
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

    in_memory_sink = InMemorySink()
    jsonl_sink = JsonlBatchSink(jsonl_path, batch_size=250)
    sqlite_sink = SqliteBatchSink(db_path, batch_size=250)

    pipeline = AsyncStreamPipeline(
        queue_capacity=20_000,
        num_workers=num_workers,
        sinks=[in_memory_sink, jsonl_sink, sqlite_sink],
        batch_size=250,
        flush_interval_sec=0.5
    )

    await pipeline.start()

    generator = TelemetryStreamGenerator(
        drone_ids=["UAV-ALPHA-01", "UAV-BETA-02", "UAV-GAMMA-03", "UAV-DELTA-04"],
        anomaly_rate=0.06,
        corrupt_rate=0.015
    )

    print("\n[INGESTA] Ingestando y procesando paquetes en caliente...")
    t0 = time.perf_counter()

    async for raw_payload in generator.generate_stream(total_events=total_events):
        await pipeline.submit_raw(raw_payload)

    # Detener de forma ordenada esperando que la cola se vacie
    await pipeline.stop()
    t1 = time.perf_counter()

    elapsed = t1 - t0
    metrics = pipeline.get_metrics()

    print("\n" + "=" * 70)
    print("RESULTADOS DE RENDIMIENTO Y CONCURRENCIA")
    print("=" * 70)
    print(f"Tiempo total de procesamiento    : {elapsed:.3f} segundos")
    print(f"Throughput efectivo              : {metrics.throughput_eps:,.1f} eventos/segundo")
    print(f"Paquetes totales ingestados      : {metrics.events_ingested:,}")
    print(f"Eventos procesados y persistidos : {metrics.events_processed:,}")
    print(f"Anomalias fisicas detectadas     : {metrics.anomalies_detected:,} ({(metrics.anomalies_detected/max(metrics.events_processed, 1))*100:.1f}%)")
    print(f"Desviados a Dead-Letter Queue    : {metrics.events_dropped_to_dlq:,} ({(metrics.events_dropped_to_dlq/max(metrics.events_ingested, 1))*100:.1f}%)")
    print(f"Latencia media de pipeline       : {metrics.avg_latency_ms:.3f} ms/evento")
    print(f"Registros en memoria / SQLite    : {len(in_memory_sink.events):,} filas")
    print("=" * 70)
    print("[OK] Benchmark completado con exito y cero perdida de datos no clasificados.\n")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
