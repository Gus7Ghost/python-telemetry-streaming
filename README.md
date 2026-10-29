# PyStreamPulse: Framework Asíncrono de Streaming de Telemetría y Detección de Anomalías

[![Python](https://img.shields.io/badge/Python-3.12%2B-blue.svg)](https://www.python.org/)
[![Concurrency](https://img.shields.io/badge/Concurrency-asyncio-darkgreen.svg)](https://docs.python.org/3/library/asyncio.html)
[![Validation](https://img.shields.io/badge/Data%20Validation-Pydantic%20v2-e92063.svg)](https://docs.pydantic.dev/)
[![Throughput](https://img.shields.io/badge/Throughput-6%2C200%2B%20events%2Fsec-orange.svg)](#-benchmark-de-rendimiento)
[![Tests](https://img.shields.io/badge/Tests-8%2F8%20Passed-brightgreen.svg)](#-pruebas-automatizadas)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Framework asíncrono en **Python 3.12** diseñado para la ingesta masiva, validación estricta de esquemas, detección de anomalías en tiempo real y persistencia políglota de flujos de telemetría de alta frecuencia provenientes de enjambres de vehículos aéreos no tripulados (**UAV / Drones**) y sensores industriales.

Desarrollado con base en la experiencia de investigación en el **LabIAR (Laboratorio de Inteligencia Artificial y Robótica - UNI)**.

---

## 📌 Contexto y Desafíos de Ingeniería

En misiones de vuelo autónomo y monitoreo de maquinaria crítica, los flujos de telemetría generan cientos de paquetes por segundo por dispositivo (altitud, cabeceo, alabeo, voltaje de baterías LiPo, velocidad y temperatura).

### Problemas resueltos por el framework:
1. **Control de contrapresión (Backpressure):** Cola de entrada acotada (`asyncio.Queue(maxsize=N)`) que previene el desbordamiento de memoria ante ráfagas extremas de tráfico.
2. **Validación estricta de tipos a costo casi cero:** Modelado con **Pydantic v2** para coerción, validación de invariantes físicas y serialización optimizada en C (Rust core).
3. **Detección de anomalías en caliente (Dual Layer):**
   - **Hard Thresholds:** Violación de límites de seguridad (e.g. caída de batería $< 10.2\text{ V}$, inclinación excesiva $> 45^\circ$, altitud de vuelo no autorizada).
   - **Rolling Z-Score:** Ventana deslizante $O(1)$ que evalúa desviaciones estadísticas súbitas ($Z > 3.0$).
4. **Tolerancia a fallos con Dead-Letter Queue (DLQ):** Los paquetes corruptos o malformados se aíslan automáticamente en una cola de contingencia para análisis forense, sin degradar el throughput del pipeline principal.
5. **Persistencia concurrente Multi-Sink:** Escritura no bloqueante por lotes a archivos JSON Lines (`.jsonl`) y bases de datos relacionales indexadas (**SQLite** via `asyncio.to_thread`).

---

## 🏛️ Arquitectura del Pipeline Asíncrono

```
                              [ Generador / Sensores UAV ]
                                           │
                                           ▼ (submit_raw)
                        ┌─────────────────────────────────────┐
                        │    Cola Acotada de Ingesta (Queue)   │
                        └──────────────────┬──────────────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │                                             │
          [ Fallo de Validación ]                         [ Paquete Válido ]
                    │                                             │
                    ▼                                             ▼
        ┌───────────────────────┐                    ┌─────────────────────────┐
        │   Dead-Letter Queue   │                    │     Worker Pool (x4)    │
        │         (DLQ)         │                    │ (Procesamiento Concur.) │
        └───────────────────────┘                    └────────────┬────────────┘
                                                                  │
                                                ┌─────────────────┴─────────────────┐
                                                ▼                                   ▼
                                      [ Hard Thresholds ]                  [ Rolling Z-Score ]
                                                │                                   │
                                                └─────────────────┬─────────────────┘
                                                                  ▼
                                                    ┌───────────────────────────┐
                                                    │ Batch Dispatcher / Sinks  │
                                                    └─────────────┬─────────────┘
                                                                  │
                                      ┌───────────────────────────┼───────────────────────────┐
                                      ▼                           ▼                           ▼
                              [ InMemorySink ]           [ JsonlBatchSink ]          [ SqliteBatchSink ]
```

---

## ⚡ Benchmark de Rendimiento en Tiempo Real

Resultados verificados de estrés ejecutando 5,000 eventos de telemetría continuos con 4 workers asíncronos y 3 sinks activos simultáneamente:

```text
======================================================================
PYSTREAMPULSE - BENCHMARK DE STREAMING ASINCRONO DE TELEMETRIA
======================================================================
Eventos totales a transmitir      : 5,000
Workers asincronos concurrentes   : 4
Directorio de persistencia        : output

[INGESTA] Ingestando y procesando paquetes en caliente...

======================================================================
RESULTADOS DE RENDIMIENTO Y CONCURRENCIA
======================================================================
Tiempo total de procesamiento    : 0.780 segundos
Throughput efectivo              : 6,293.4 eventos/segundo
Paquetes totales ingestados      : 5,000
Eventos procesados y persistidos : 4,910
Anomalias fisicas detectadas     : 287 (5.8%)
Desviados a Dead-Letter Queue    : 90 (1.8%)
Latencia media de pipeline       : 347.188 ms/evento
Registros en memoria / SQLite    : 4,910 filas
======================================================================
[OK] Benchmark completado con exito y cero perdida de datos no clasificados.
```

---

## 🧪 Pruebas Automatizadas (Pytest)

Pruebas asíncronas exhaustivas con `pytest` y `pytest-asyncio`:
- **Modelos:** Validación de esquemas Pydantic v2, sanitización de strings y rechazo de campos inválidos.
- **Procesador:** Cálculo incremental $O(1)$ de media y desviación estándar en ventanas deslizantes; detección de anomalías por umbrales fijos y dinámicos.
- **Pipeline:** Flujo end-to-end con múltiples workers, enrutamiento a DLQ ante datos malformados y apagado limpio (*Graceful Shutdown*).

### Ejecución de Pruebas:
```bash
python -m pytest tests/
```

### Resultado de la Ejecución:
```text
collected 8 items

tests\test_models.py ...                                                 [ 37%]
tests\test_pipeline.py ..                                                [ 62%]
tests\test_processor.py ...                                              [100%]

============================== 8 passed in 0.19s ==============================
```

---

## 💻 Instrucciones de Instalación y Uso

### Prerrequisitos:
- Python 3.10 o superior (verificado en Python 3.12).

### 1. Clonar el repositorio
```bash
git clone https://github.com/Gus7Ghost/python-telemetry-streaming.git
cd python-telemetry-streaming
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
```

### 3. Ejecutar las pruebas unitarias
```bash
python -m pytest tests/
```

### 4. Ejecutar el Benchmark de Carga
```bash
python pystreampulse/benchmark.py
```
*Los datos procesados se almacenarán automáticamente en `output/benchmark_telemetry.jsonl` y en la base analítica SQLite `output/benchmark_telemetry.db`.*

---

## 👤 Autor

**Gustavo Bryan López Cosme**  
- **Rol:** Estudiante de 9no ciclo de Ingeniería de Sistemas — Universidad Nacional de Ingeniería (UNI, Lima, Perú)
- **Investigador:** LabIAR (Laboratorio de Inteligencia Artificial y Robótica - UNI)
- **Email:** [glopezc@uni.pe](mailto:glopezc@uni.pe)
- **GitHub:** [@Gus7Ghost](https://github.com/Gus7Ghost)
- **LinkedIn:** [Perfil en LinkedIn](https://www.linkedin.com)
