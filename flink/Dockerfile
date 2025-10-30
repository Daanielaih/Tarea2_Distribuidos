FROM python:3.9-slim

WORKDIR /app

# Copiar utils
COPY utils.py /app/
COPY flink/flink_job.py /app/

# Instalar dependencias
RUN pip install --no-cache-dir kafka-python pandas numpy google-generativeai

CMD ["python", "flink_job.py"]

