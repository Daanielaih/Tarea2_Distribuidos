# -*- coding: utf-8 -*-
"""Reintentador para errores de sobrecarga"""

import json
import time
from kafka import KafkaConsumer, KafkaProducer

topic_overload = "llm-errors-overload"
topic_pending = "questions-pending"
max_retries = 5

def wait_for_kafka(bootstrap_servers, max_retries=30):
    """Espera a que Kafka esté disponible"""
    from kafka import KafkaProducer
    for i in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                api_version=(0, 10),
                request_timeout_ms=1000
            )
            producer.close()
            print("✓ Kafka está disponible")
            return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"Esperando Kafka... ({i+1}/{max_retries})")
                import time
                time.sleep(2)
            else:
                print(f"✗ No se pudo conectar a Kafka: {e}")
                return False
    return False

def main():
    bootstrap_servers = "kafka:29092"
    
    # Esperar a que Kafka esté listo
    if not wait_for_kafka(bootstrap_servers):
        print("Error: No se pudo conectar a Kafka. Saliendo...")
        return
    
    consumer = KafkaConsumer(
        topic_overload,
        bootstrap_servers=[bootstrap_servers],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id="overload-retry-group",
        auto_offset_reset="earliest"
    )
    
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    print("Reintentador de sobrecarga iniciado...")
    
    try:
        for message in consumer:
            question_data = message.value
            question_index = question_data["question_index"]
            retry_count = question_data.get("retry_count", 0)
            
            if retry_count >= max_retries:
                print(f"✗ Máximo de reintentos alcanzado para pregunta {question_index}")
                continue
            
            retry_after = question_data.get("retry_after", time.time())
            wait_time = max(0, retry_after - time.time())
            
            if wait_time > 0:
                print(f"Esperando {wait_time:.2f}s antes de reintentar pregunta {question_index}...")
                time.sleep(wait_time)
            
            # Reenviar a tópico de preguntas pendientes
            question_data.pop("retry_after", None)
            producer.send(
                topic_pending,
                key=str(question_index),
                value=question_data
            )
            print(f"✓ Reenviada pregunta {question_index} (intento {retry_count + 1})")
            producer.flush()
            
    except KeyboardInterrupt:
        print("\nDeteniendo reintentador...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()

