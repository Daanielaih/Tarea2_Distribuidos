FROM python:3.9-slim

WORKDIR /app

# Copiar utils
COPY utils.py /app/
COPY services/llm_consumer/main.py /app/

# Instalar dependencias
RUN pip install --no-cache-dir kafka-python pandas google-generativeai numpy

CMD ["python", "main.py"]

