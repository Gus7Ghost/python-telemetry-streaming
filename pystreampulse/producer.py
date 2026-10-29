import asyncio
from datetime import datetime, timezone
import random
from typing import AsyncGenerator, Dict, List
from pystreampulse.models import RawTelemetryPacket, TelemetrySourceType


class TelemetryStreamGenerator:
    """
    Generador de flujo sintético de telemetría para UAVs / drones industriales.
    Simula dinámicas de vuelo reales: altitud, cabeceo (pitch), alabeo (roll),
    voltaje de batería LiPo y velocidad en m/s.
    """

    def __init__(
        self,
        drone_ids: List[str] = None,
        anomaly_rate: float = 0.05,
        corrupt_rate: float = 0.02,
    ):
        self.drone_ids = drone_ids or ["DRONE-ALPHA-01", "DRONE-BETA-02", "DRONE-GAMMA-03"]
        self.anomaly_rate = anomaly_rate
        self.corrupt_rate = corrupt_rate
        self._seq = 0

    async def generate_stream(
        self,
        total_events: int = 1000,
        delay_seconds: float = 0.0,
    ) -> AsyncGenerator[Dict, None]:
        """Produce un generador asíncrono de eventos de telemetría."""
        for _ in range(total_events):
            self._seq += 1
            device = random.choice(self.drone_ids)

            # Inyección de carga corrupta (para probar Dead-Letter Queue)
            if random.random() < self.corrupt_rate:
                yield {
                    "device_id": "", # Inválido: min_length=3
                    "metrics": "invalid_format_string",
                    "corrupt_flag": True
                }
                continue

            # Inyección de anomalía física
            is_anomaly = random.random() < self.anomaly_rate

            if is_anomaly:
                # Simular falla de batería o inclinación excesiva
                battery = round(random.uniform(9.0, 10.1), 2) # Por debajo de 10.2V
                pitch = round(random.uniform(50.0, 75.0), 2)  # Por encima de 45 deg
                altitude = round(random.uniform(520.0, 600.0), 1) # Por encima de 500m
            else:
                battery = round(random.uniform(14.2, 16.8), 2)
                pitch = round(random.uniform(-15.0, 15.0), 2)
                altitude = round(random.uniform(30.0, 120.0), 1)

            speed = round(random.uniform(5.0, 22.0), 1)
            roll = round(random.uniform(-12.0, 12.0), 2)
            temperature = round(random.uniform(28.0, 48.0), 1)

            payload = {
                "device_id": device,
                "source_type": TelemetrySourceType.UAV_DRONE.value,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metrics": {
                    "altitude_m": altitude,
                    "battery_v": battery,
                    "pitch_deg": pitch,
                    "roll_deg": roll,
                    "speed_ms": speed,
                    "temperature_c": temperature,
                },
                "sequence_id": self._seq,
            }

            yield payload

            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
