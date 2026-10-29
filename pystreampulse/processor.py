from collections import deque
from datetime import datetime, timezone
import math
from typing import Dict, List, Optional, Tuple
from pystreampulse.models import ProcessedTelemetryEvent, RawTelemetryPacket, TelemetrySourceType


class RollingStats:
    """Calculador de media y desviación estándar incremental en ventana deslizante O(1)."""

    def __init__(self, window_size: int = 50):
        self.window_size = window_size
        self.values: deque = deque(maxlen=window_size)

    def add(self, value: float) -> None:
        self.values.append(value)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def mean(self) -> float:
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)

    @property
    def std_dev(self) -> float:
        if len(self.values) < 2:
            return 0.0
        mean = self.mean
        variance = sum((x - mean) ** 2 for x in self.values) / (len(self.values) - 1)
        return math.sqrt(variance)


class TelemetryProcessor:
    """
    Procesador de eventos en tiempo real:
    - Verificación de límites físicos críticos (Hard Thresholds).
    - Detección de picos estadísticos anómalos (Rolling Z-Score > 3.0).
    - Cálculo de latencia extremo a extremo.
    """

    # Límites físicos por métrica (Min, Max)
    DEFAULT_THRESHOLDS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
        "altitude_m": (0.0, 500.0),          # Rango de vuelo civil seguro
        "battery_v": (10.2, 26.0),           # Umbral crítico de descarga LiPo
        "pitch_deg": (-45.0, 45.0),          # Inclinación límite de cabeceo
        "roll_deg": (-45.0, 45.0),           # Inclinación límite de alabeo
        "speed_ms": (0.0, 40.0),             # Velocidad máxima operativa
        "temperature_c": (-20.0, 80.0),      # Temperatura térmica de operación
    }

    def __init__(self, window_size: int = 50, z_threshold: float = 3.0):
        self.window_size = window_size
        self.z_threshold = z_threshold
        # Key: (device_id, metric_name) -> RollingStats
        self._windows: Dict[Tuple[str, str], RollingStats] = {}

    def process(self, packet: RawTelemetryPacket) -> ProcessedTelemetryEvent:
        now = datetime.now(timezone.utc)
        # Asegurar compatibilidad de timezones para el cálculo de latencia
        packet_time = packet.timestamp if packet.timestamp.tzinfo else packet.timestamp.replace(tzinfo=timezone.utc)
        latency_ms = max(0.0, (now - packet_time).total_seconds() * 1000.0)

        anomaly_reasons: List[str] = []

        for metric_name, value in packet.metrics.items():
            # 1. Regla de límites fijos (Hard bounds)
            if metric_name in self.DEFAULT_THRESHOLDS:
                min_val, max_val = self.DEFAULT_THRESHOLDS[metric_name]
                if min_val is not None and value < min_val:
                    anomaly_reasons.append(
                        f"Límite inferior violado en {metric_name}: {value:.2f} < {min_val:.2f}"
                    )
                if max_val is not None and value > max_val:
                    anomaly_reasons.append(
                        f"Límite superior excedido en {metric_name}: {value:.2f} > {max_val:.2f}"
                    )

            # 2. Detección estadística adaptativa (Z-score en ventana deslizante)
            key = (packet.device_id, metric_name)
            if key not in self._windows:
                self._windows[key] = RollingStats(window_size=self.window_size)

            stats = self._windows[key]
            if stats.count >= 10 and stats.std_dev > 1e-4:
                z_score = abs(value - stats.mean) / stats.std_dev
                if z_score > self.z_threshold:
                    anomaly_reasons.append(
                        f"Pico anómalo en {metric_name} (Z={z_score:.2f} > {self.z_threshold:.1f}, media={stats.mean:.2f})"
                    )

            # Actualizar ventana
            stats.add(value)

        is_anomalous = len(anomaly_reasons) > 0

        return ProcessedTelemetryEvent(
            device_id=packet.device_id,
            source_type=packet.source_type,
            event_timestamp=packet_time,
            processed_at=now,
            metrics=packet.metrics,
            sequence_id=packet.sequence_id,
            is_anomalous=is_anomalous,
            anomaly_reasons=anomaly_reasons,
            latency_ms=round(latency_ms, 3)
        )
