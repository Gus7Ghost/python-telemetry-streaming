import asyncio
import pytest
from pystreampulse.models import RawTelemetryPacket
from pystreampulse.pipeline import AsyncStreamPipeline
from pystreampulse.sinks import InMemorySink


@pytest.mark.asyncio
async def test_pipeline_end_to_end_ingestion():
    sink = InMemorySink()
    pipeline = AsyncStreamPipeline(
        queue_capacity=50,
        num_workers=2,
        sinks=[sink],
        batch_size=5,
        flush_interval_sec=0.1
    )

    await pipeline.start()

    # Ingest 10 valid packets
    for i in range(10):
        ok = await pipeline.submit_raw({
            "device_id": f"UAV-{i}",
            "metrics": {"altitude_m": 40.0 + i, "battery_v": 15.0},
            "sequence_id": i
        })
        assert ok is True

    await pipeline.stop()

    assert len(sink.events) == 10
    metrics = pipeline.get_metrics()
    assert metrics.events_processed == 10
    assert metrics.events_dropped_to_dlq == 0


@pytest.mark.asyncio
async def test_pipeline_routes_corrupt_packet_to_dlq():
    sink = InMemorySink()
    pipeline = AsyncStreamPipeline(
        queue_capacity=50,
        num_workers=2,
        sinks=[sink],
        batch_size=5
    )

    await pipeline.start()

    # Submit valid packet
    await pipeline.submit_raw({
        "device_id": "UAV-VALID",
        "metrics": {"altitude_m": 50.0}
    })

    # Submit corrupted packet (invalid type for metrics)
    bad_ok = await pipeline.submit_raw({
        "device_id": "UAV-BAD",
        "metrics": "not_a_dictionary"
    })
    assert bad_ok is False

    await pipeline.stop()

    assert len(sink.events) == 1
    assert len(pipeline.dlq_records) == 1
    assert "Validación fallida" in pipeline.dlq_records[0].error_reason
