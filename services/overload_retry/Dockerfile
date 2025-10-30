FROM python:3.9-slim

WORKDIR /app

COPY services/overload_retry/main.py /app/

RUN pip install --no-cache-dir kafka-python

CMD ["python", "main.py"]

