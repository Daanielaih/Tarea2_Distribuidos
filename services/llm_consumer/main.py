# -*- coding: utf-8 -*-
"""Consumidor de Kafka que procesa preguntas y llama al LLM"""

import json
import time
import random
from kafka import KafkaConsumer, KafkaProducer
from google.generativeai import GenerativeModel
import google.generativeai as genai
from utils import generate_prompt

# Configuración
genai.configure(api_key="AIzaSyA5aNjtIegCftM67zsC8ahEgGlEISbcmVc")
model = GenerativeModel("gemini-2.5-flash")

# Tópicos de Kafka
topic_pending = "questions-pending"
topic_success = "llm-responses-success"
topic_rate_limit = "llm-errors-rate-limit"
topic_overload = "llm-errors-overload"

def exponential_backoff(retry_count, base_delay=1):
    """Calcula el delay para exponential backoff"""
    return base_delay * (2 ** retry_count) + random.uniform(0, 1)

def call_llm(prompt, max_retries=3):
    """
    Llama al LLM con manejo de errores
    Retorna (success, response, error_type)
    """
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            if response and response.text:
                return True, response.text, None
            else:
                # Contenido bloqueado o vacío
                return False, None, "blocked"
        except Exception as e:
            error_str = str(e).lower()
            
            # Detectar tipo de error
            if "quota" in error_str or "quota_exceeded" in error_str or "429" in error_str:
                return False, None, "rate_limit"
            elif "overload" in error_str or "503" in error_str or "500" in error_str:
                return False, None, "overload"
            else:
                # Otro tipo de error, reintentar con backoff
                if attempt < max_retries - 1:
                    time.sleep(exponential_backoff(attempt))
                    continue
                return False, None, "unknown"
    
    return False, None, "unknown"

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
    # Esperar a que Kafka esté listo
    bootstrap_servers = "kafka:29092"
    if not wait_for_kafka(bootstrap_servers):
        print("Error: No se pudo conectar a Kafka. Saliendo...")
        return
    
    # Crear consumidor
    consumer = KafkaConsumer(
        topic_pending,
        bootstrap_servers=[bootstrap_servers],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id="llm-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    # Crear productor para enviar resultados
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    print("Consumidor LLM iniciado. Esperando preguntas...")
    
    try:
        for message in consumer:
            question_data = message.value
            question_index = question_data["question_index"]
            retry_count = question_data.get("retry_count", 0)
            
            print(f"\nProcesando pregunta {question_index} (intento {retry_count + 1})...")
            
            # Generar prompt
            prompt = generate_prompt(
                question_data["question_title"],
                question_data["question_content"]
            )
            
            # Llamar al LLM
            success, response, error_type = call_llm(prompt)
            
            if success:
                # Respuesta exitosa
                result_message = {
                    "question_index": question_index,
                    "question_title": question_data["question_title"],
                    "question_content": question_data["question_content"],
                    "human_answer": question_data["human_answer"],
                    "llm_answer": response,
                    "timestamp": question_data["timestamp"],
                    "retry_count": retry_count
                }
                
                producer.send(
                    topic_success,
                    key=str(question_index),
                    value=result_message
                )
                print(f"✓ Respuesta exitosa para pregunta {question_index}")
                
            else:
                # Manejar errores según tipo
                if error_type == "rate_limit":
                    # Reenviar a tópico de rate limit con delay
                    delay = exponential_backoff(retry_count, base_delay=5)
                    question_data["retry_count"] = retry_count + 1
                    question_data["retry_after"] = time.time() + delay
                    
                    # Enviar a tópico de rate limit
                    producer.send(
                        topic_rate_limit,
                        key=str(question_index),
                        value=question_data
                    )
                    print(f"⚠ Rate limit detectado para pregunta {question_index}. Reintento en {delay:.2f}s")
                    
                elif error_type == "overload":
                    # Reenviar a tópico de overload con delay más corto
                    delay = exponential_backoff(retry_count, base_delay=2)
                    question_data["retry_count"] = retry_count + 1
                    question_data["retry_after"] = time.time() + delay
                    
                    producer.send(
                        topic_overload,
                        key=str(question_index),
                        value=question_data
                    )
                    print(f"⚠ Sobrecarga detectada para pregunta {question_index}. Reintento en {delay:.2f}s")
                    
                else:
                    # Error desconocido o bloqueado
                    print(f"✗ Error desconocido para pregunta {question_index}: {error_type}")
            
            producer.flush()
            
    except KeyboardInterrupt:
        print("\nDeteniendo consumidor...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()

