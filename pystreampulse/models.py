from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid
from pydantic import BaseModel, Field, field_validator


class TelemetrySourceType(str, Enum):
    UAV_DRONE = "uav_drone"
    IOT_ROBOT = "iot_robot"
    WEATHER_STATION = "weather_station"
    INDUSTRIAL_MOTOR = "industrial_motor"


class RawTelemetryPacket(BaseModel):
    """
    Representa el paquete sin procesar recibido desde sensores o vehículos aéreos (UAV).
    Valida tipos, rangos físicos y parseo estricto con Pydantic v2.
    """
    device_id: str = Field(..., min_length=3, max_length=64, description="Identificador único del dispositivo emisor")
    source_type: TelemetrySourceType = Field(default=TelemetrySourceType.UAV_DRONE)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metrics: Dict[str, float] = Field(..., min_length=1, description="Mapa de variables físicas censadas")
    sequence_id: int = Field(default=0, ge=0)

    @field_validator("device_id")
    @classmethod
    def clean_device_id(cls, v: str) -> str:
        cleaned = v.strip().upper()
        if not cleaned:
            raise ValueError("El device_id no puede ser vacío ni contener solo espacios")
        return cleaned


class ProcessedTelemetryEvent(BaseModel):
    """
    Evento enriquecido luego de pasar por el pipeline de validación,
    cálculo de latencia y detección de anomalías en ventana deslizante.
    """
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    device_id: str
    source_type: TelemetrySourceType
    event_timestamp: datetime
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metrics: Dict[str, float]
    sequence_id: int
    is_anomalous: bool = False
    anomaly_reasons: List[str] = Field(default_factory=list)
    latency_ms: float = 0.0


class DeadLetterEvent(BaseModel):
    """
    Registro para paquetes corruptos, con errores de serialización o violaciones de invariantes,
    enviados a la Dead-Letter Queue (DLQ) para análisis forense sin detener el pipeline.
    """
    dlq_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    raw_payload: Any
    error_reason: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class PipelineMetrics(BaseModel):
    """
    Métricas de rendimiento operacional del pipeline en tiempo real.
    """
    events_ingested: int = 0
    events_processed: int = 0
    events_dropped_to_dlq: int = 0
    anomalies_detected: int = 0
    avg_latency_ms: float = 0.0
    throughput_eps: float = 0.0
    queue_backpressure_ratio: float = 0.0
