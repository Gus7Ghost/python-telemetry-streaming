from abc import ABC, abstractmethod
import asyncio
from datetime import datetime
import json
import os
import sqlite3
from typing import Any, List
from pystreampulse.models import ProcessedTelemetryEvent


class BaseSink(ABC):
    @abstractmethod
    async def write(self, events: List[ProcessedTelemetryEvent]) -> None:
        pass

    @abstractmethod
    async def flush(self) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class InMemorySink(BaseSink):
    """Sink en memoria utilizado para testing unitario y benchmarks sin latencia de disco."""

    def __init__(self):
        self.events: List[ProcessedTelemetryEvent] = []

    async def write(self, events: List[ProcessedTelemetryEvent]) -> None:
        self.events.extend(events)

    async def flush(self) -> None:
        pass

    async def close(self) -> None:
        pass


class JsonlBatchSink(BaseSink):
    """
    Sink asíncrono con buffering que escribe eventos en formato JSON Lines (JSONL).
    Permite serialización eficiente y partición por disco.
    """

    def __init__(self, file_path: str, batch_size: int = 100):
        self.file_path = file_path
        self.batch_size = batch_size
        self._buffer: List[ProcessedTelemetryEvent] = []
        self._lock = asyncio.Lock()

        # Asegurar directorio
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)

    async def write(self, events: List[ProcessedTelemetryEvent]) -> None:
        async with self._lock:
            self._buffer.extend(events)
            if len(self._buffer) >= self.batch_size:
                await self._flush_internal()

    async def flush(self) -> None:
        async with self._lock:
            if self._buffer:
                await self._flush_internal()

    async def _flush_internal(self) -> None:
        if not self._buffer:
            return

        to_write = self._buffer[:]
        self._buffer.clear()

        def _sync_write():
            with open(self.file_path, "a", encoding="utf-8") as f:
                for ev in to_write:
                    f.write(ev.model_dump_json() + "\n")

        await asyncio.to_thread(_sync_write)

    async def close(self) -> None:
        await self.flush()


class SqliteBatchSink(BaseSink):
    """
    Sink analítico respaldado en SQLite. Escribe lotes de eventos mediante
    transacciones de inserción masiva en hilos de background para no bloquear el event loop.
    """

    def __init__(self, db_path: str, batch_size: int = 100):
        self.db_path = db_path
        self.batch_size = batch_size
        self._buffer: List[ProcessedTelemetryEvent] = []
        self._lock = asyncio.Lock()

        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_events (
                    event_id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    sequence_id INTEGER NOT NULL,
                    is_anomalous INTEGER NOT NULL,
                    anomaly_reasons TEXT,
                    latency_ms REAL NOT NULL
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_device ON telemetry_events(device_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp ON telemetry_events(event_timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_anomaly ON telemetry_events(is_anomalous)")
            conn.commit()

    async def write(self, events: List[ProcessedTelemetryEvent]) -> None:
        async with self._lock:
            self._buffer.extend(events)
            if len(self._buffer) >= self.batch_size:
                await self._flush_internal()

    async def flush(self) -> None:
        async with self._lock:
            if self._buffer:
                await self._flush_internal()

    async def _flush_internal(self) -> None:
        if not self._buffer:
            return

        records = [
            (
                ev.event_id,
                ev.device_id,
                ev.source_type.value,
                ev.event_timestamp.isoformat(),
                ev.processed_at.isoformat(),
                json.dumps(ev.metrics),
                ev.sequence_id,
                1 if ev.is_anomalous else 0,
                "; ".join(ev.anomaly_reasons) if ev.anomaly_reasons else None,
                ev.latency_ms
            )
            for ev in self._buffer
        ]
        self._buffer.clear()

        def _sync_insert():
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany("""
                    INSERT INTO telemetry_events (
                        event_id, device_id, source_type, event_timestamp,
                        processed_at, metrics_json, sequence_id,
                        is_anomalous, anomaly_reasons, latency_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                conn.commit()

        await asyncio.to_thread(_sync_insert)

    async def close(self) -> None:
        await self.flush()
