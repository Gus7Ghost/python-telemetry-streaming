import pytest
from pydantic import ValidationError
from pystreampulse.models import RawTelemetryPacket, TelemetrySourceType


def test_raw_telemetry_valid_creation():
    packet = RawTelemetryPacket(
        device_id=" drone-x1 ",
        source_type=TelemetrySourceType.UAV_DRONE,
        metrics={"altitude_m": 120.5, "battery_v": 15.4}
    )
    assert packet.device_id == "DRONE-X1"  # Cleaned and uppercased
    assert packet.metrics["altitude_m"] == 120.5
    assert packet.source_type == TelemetrySourceType.UAV_DRONE


def test_raw_telemetry_invalid_device_id_raises():
    with pytest.raises(ValidationError):
        RawTelemetryPacket(
            device_id="  ",
            metrics={"altitude_m": 100.0}
        )


def test_raw_telemetry_empty_metrics_raises():
    with pytest.raises(ValidationError):
        RawTelemetryPacket(
            device_id="DRONE-01",
            metrics={}
        )
