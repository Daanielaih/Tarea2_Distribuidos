# -*- coding: utf-8 -*-
"""Consumidor de almacenamiento que persiste resultados válidos"""

import json
import sqlite3
import os
from kafka import KafkaConsumer

def insert_or_update(conn, query):
    """Inserta o actualiza una entrada en la BD"""
    c = conn.cursor()
    
    # Verificar si la pregunta ya existe
    c.execute("SELECT id, count FROM queries WHERE question_index=?", (query['index'],))
    row = c.fetchone()
    
    if row:
        # Actualizar contador, última respuesta LLM y score
        c.execute('''
            UPDATE queries
            SET count = ?, llm_answer = ?, similarity_score = ?
            WHERE id = ?
        ''', (row[1]+1, query['llm_answer'], query['score'], row[0]))
    else:
        # Insertar nueva fila
        c.execute('''
            INSERT INTO queries (question_index, question_title, question_content,
                                 human_answer, llm_answer, similarity_score, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (query['index'], query['title'], query['content'],
              query['human_answer'], query['llm_answer'], query['score'], 1))
    
    conn.commit()

def init_database(db_path):
    """Inicializa la base de datos si no existe"""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question_index INTEGER,
            question_title TEXT,
            question_content TEXT,
            human_answer TEXT,
            llm_answer TEXT,
            similarity_score REAL,
            count INTEGER DEFAULT 1
        )
    ''')
    conn.commit()
    return conn

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
    topic_valid = "results-valid"
    db_path = "/app/data/queries.db"
    
    # Asegurar que el directorio existe
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Inicializar BD
    conn = init_database(db_path)
    
    # Crear consumidor
    consumer = KafkaConsumer(
        topic_valid,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None,
        group_id="storage-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )
    
    print("Consumidor de almacenamiento iniciado. Esperando resultados válidos...")
    
    try:
        for message in consumer:
            result_data = message.value
            
            query_data = {
                "index": result_data["question_index"],
                "title": result_data["question_title"],
                "content": result_data["question_content"],
                "human_answer": result_data["human_answer"],
                "llm_answer": result_data["llm_answer"],
                "score": result_data["similarity_score"]
            }
            
            # Guardar en BD
            try:
                insert_or_update(conn, query_data)
                print(f"✓ Guardada pregunta {query_data['index']} con score {query_data['score']:.4f}")
            except Exception as e:
                print(f"✗ Error guardando pregunta {query_data['index']}: {e}")
            
    except KeyboardInterrupt:
        print("\nDeteniendo consumidor...")
    finally:
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()

