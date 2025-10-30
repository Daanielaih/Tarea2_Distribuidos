# Sistema Distribuido de Análisis de Preguntas y Respuestas

Este proyecto implementa un sistema distribuido asíncrono para procesar preguntas usando un LLM (Gemini), calcular scores de calidad y gestionar reintentos mediante Apache Kafka y procesamiento de flujos.

## Arquitectura

El sistema está compuesto por los siguientes componentes:

1. **Traffic Generator**: Genera preguntas simuladas y las envía a Kafka
2. **LLM Consumer**: Consume preguntas de Kafka, las procesa con Gemini y maneja errores
3. **Rate Limit Retry**: Reintenta preguntas que fallaron por límite de cuota
4. **Overload Retry**: Reintenta preguntas que fallaron por sobrecarga
5. **Flink Score Processor**: Calcula scores de calidad y decide si reenviar preguntas
6. **Storage Consumer**: Persiste resultados válidos en SQLite

## Requisitos Previos

- Docker y Docker Compose instalados
- Archivo `test.csv` con el dataset en el directorio `data/`
- API Key de Google Gemini configurada en `utils.py`

## Estructura del Proyecto

```
.
├── data/
│   ├── test.csv              # Dataset de preguntas
│   └── queries.db            # Base de datos SQLite (se crea automáticamente)
├── services/
│   ├── traffic_generator/     # Generador de tráfico
│   ├── llm_consumer/         # Consumidor LLM
│   ├── rate_limit_retry/      # Reintentador rate limit
│   ├── overload_retry/       # Reintentador overload
│   └── storage_consumer/     # Consumidor de almacenamiento
├── flink/
│   └── flink_job.py          # Procesador de scores
├── utils.py                  # Utilidades compartidas
├── distri_base.py           # Código original (no modificar)
├── docker-compose.yml        # Orquestación de servicios
└── requirements.txt          # Dependencias Python

```

## Instalación y Ejecución

1. **Preparar el dataset**:
   ```bash
   mkdir -p data
   # Colocar tu archivo test.csv en el directorio data/
   ```

2. **Configurar API Key**:
   Edita `utils.py` y configura tu API key de Gemini:
   ```python
   genai.configure(api_key="TU_API_KEY_AQUI")
   ```

3. **Construir e iniciar los servicios**:
   ```bash
   docker-compose up --build
   ```

4. **Ejecutar generador de tráfico** (en otra terminal):
   ```bash
   docker-compose run --rm traffic-generator
   ```

5. **Monitorear los logs**:
   ```bash
   docker-compose logs -f
   ```

## Verificar Resultados

Para ver los resultados almacenados en la base de datos:

```bash
docker-compose exec storage-consumer python -c "
import sqlite3
import pandas as pd
conn = sqlite3.connect('/app/data/queries.db')
df = pd.read_sql('SELECT * FROM queries', conn)
print(df)
conn.close()
"
```

## Configuración

### Umbral de Calidad

El umbral de calidad está configurado en `flink/flink_job.py`:
```python
QUALITY_THRESHOLD = 0.6  # Score mínimo para considerar válida
MAX_RETRIES = 3  # Máximo número de reintentos
```

### Tópicos de Kafka

- `questions-pending`: Preguntas pendientes de procesar
- `llm-responses-success`: Respuestas exitosas del LLM
- `llm-errors-rate-limit`: Errores de límite de cuota
- `llm-errors-overload`: Errores de sobrecarga
- `results-valid`: Resultados válidos para almacenar

## Detener el Sistema

```bash
docker-compose down
```

Para eliminar también los volúmenes:
```bash
docker-compose down -v
```

## Troubleshooting

- Si Kafka no inicia correctamente, espera unos segundos y revisa los logs: `docker-compose logs kafka`
- Si hay errores de conexión, verifica que todos los servicios estén en la misma red Docker
- Para reiniciar un servicio específico: `docker-compose restart servicio-nombre`

