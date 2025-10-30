# -*- coding: utf-8 -*-
"""Procesador de Flink para calcular scores y decidir sobre calidad de respuestas"""

import json
import sys
import os
from kafka import KafkaConsumer, KafkaProducer

# Agregar utils al path
sys.path.append('/app')
from utils import calculate_similarity_score

# Umbral de calidad (ajustable)
QUALITY_THRESHOLD = 0.6  # Score mínimo para considerar una respuesta válida
MAX_RETRIES = 3  # Máximo número de reintentos por pregunta

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
    # Configuración
    kafka_broker = "kafka:29092"
    
    # Esperar a que Kafka esté listo
    if not wait_for_kafka(kafka_broker):
        print("Error: No se pudo conectar a Kafka. Saliendo...")
        return
    topic_success = "llm-responses-success"
    topic_valid = "results-valid"
    topic_pending = "questions-pending"
    
    # Crear consumidor
    consumer = KafkaConsumer(
        topic_success,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id="flink-score-processor-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    # Crear productores
    producer_valid = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    producer_retry = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    print(f"Procesador de Flink iniciado. Umbral de calidad: {QUALITY_THRESHOLD}")
    print(f"Esperando respuestas del LLM...")
    
    stats = {
        "total_processed": 0,
        "valid_responses": 0,
        "low_quality_retries": 0,
        "max_retries_reached": 0
    }
    
    try:
        for message in consumer:
            question_data = message.value
            question_index = question_data["question_index"]
            human_answer = question_data["human_answer"]
            llm_answer = question_data["llm_answer"]
            retry_count = question_data.get("retry_count", 0)
            
            stats["total_processed"] += 1
            
            print(f"\n[{stats['total_processed']}] Procesando pregunta {question_index} (intento {retry_count + 1})...")
            
            # Calcular score
            try:
                score = calculate_similarity_score(human_answer, llm_answer)
                print(f"Score calculado: {score:.4f}")
            except Exception as e:
                print(f"Error calculando score: {e}")
                continue
            
            # Decidir acción
            if score >= QUALITY_THRESHOLD:
                # Score válido, enviar a almacenamiento
                result_message = {
                    "question_index": question_index,
                    "question_title": question_data["question_title"],
                    "question_content": question_data["question_content"],
                    "human_answer": human_answer,
                    "llm_answer": llm_answer,
                    "similarity_score": score,
                    "retry_count": retry_count,
                    "status": "valid"
                }
                
                producer_valid.send(
                    topic_valid,
                    key=str(question_index),
                    value=result_message
                )
                stats["valid_responses"] += 1
                print(f"✓ Respuesta válida (score: {score:.4f})")
                
            else:
                # Score bajo, reenviar si no excedió reintentos
                if retry_count < MAX_RETRIES:
                    retry_message = {
                        "question_index": question_index,
                        "question_title": question_data["question_title"],
                        "question_content": question_data["question_content"],
                        "human_answer": human_answer,
                        "timestamp": question_data.get("timestamp"),
                        "retry_count": retry_count + 1
                    }
                    
                    producer_retry.send(
                        topic_pending,
                        key=str(question_index),
                        value=retry_message
                    )
                    stats["low_quality_retries"] += 1
                    print(f"⚠ Score bajo ({score:.4f}), reenviando para reintento {retry_count + 2}/{MAX_RETRIES}")
                else:
                    # Máximo de reintentos alcanzado, guardar de todas formas
                    result_message = {
                        "question_index": question_index,
                        "question_title": question_data["question_title"],
                        "question_content": question_data["question_content"],
                        "human_answer": human_answer,
                        "llm_answer": llm_answer,
                        "similarity_score": score,
                        "retry_count": retry_count,
                        "status": "max_retries_reached"
                    }
                    
                    producer_valid.send(
                        topic_valid,
                        key=str(question_index),
                        value=result_message
                    )
                    stats["max_retries_reached"] += 1
                    print(f"✗ Máximo de reintentos alcanzado (score: {score:.4f})")
            
            producer_valid.flush()
            producer_retry.flush()
            
            # Mostrar estadísticas cada 10 mensajes
            if stats["total_processed"] % 10 == 0:
                print(f"\n=== Estadísticas ===")
                print(f"Total procesadas: {stats['total_processed']}")
                print(f"Válidas: {stats['valid_responses']}")
                print(f"Reintentos: {stats['low_quality_retries']}")
                print(f"Max reintentos alcanzado: {stats['max_retries_reached']}")
                print(f"===================\n")
            
    except KeyboardInterrupt:
        print("\n\n=== Estadísticas Finales ===")
        print(f"Total procesadas: {stats['total_processed']}")
        print(f"Válidas: {stats['valid_responses']}")
        print(f"Reintentos: {stats['low_quality_retries']}")
        print(f"Max reintentos alcanzado: {stats['max_retries_reached']}")
        print("\nDeteniendo procesador...")
    finally:
        consumer.close()
        producer_valid.close()
        producer_retry.close()

if __name__ == "__main__":
    main()

