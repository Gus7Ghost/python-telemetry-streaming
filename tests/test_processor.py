from pystreampulse.models import RawTelemetryPacket, TelemetrySourceType
from pystreampulse.processor import RollingStats, TelemetryProcessor


def test_rolling_stats_mean_and_std():
    stats = RollingStats(window_size=5)
    for v in [10.0, 10.0, 10.0, 10.0]:
        stats.add(v)
    assert stats.mean == 10.0
    assert stats.std_dev == 0.0

    stats.add(20.0)
    assert stats.count == 5
    assert stats.mean == 12.0


def test_processor_flags_battery_low_anomaly():
    processor = TelemetryProcessor()
    packet = RawTelemetryPacket(
        device_id="DRONE-01",
        metrics={"battery_v": 9.8, "altitude_m": 50.0}
    )
    event = processor.process(packet)

    assert event.is_anomalous is True
    assert any("battery_v" in reason for reason in event.anomaly_reasons)


def test_processor_normal_telemetry():
    processor = TelemetryProcessor()
    packet = RawTelemetryPacket(
        device_id="DRONE-01",
        metrics={"battery_v": 15.2, "altitude_m": 50.0, "pitch_deg": 5.0}
    )
    event = processor.process(packet)

    assert event.is_anomalous is False
    assert len(event.anomaly_reasons) == 0
    assert event.latency_ms >= 0.0
