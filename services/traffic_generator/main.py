# -*- coding: utf-8 -*-
"""Generador de tráfico que envía preguntas a Kafka"""

import json
import time
import sqlite3
from kafka import KafkaProducer
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from utils import load_dataset

class TrafficGenerator:
    def __init__(self, dataset, distribution="uniform", lam=5, uniform_low=1, uniform_high=5, seed=42, start_time=None):
        """
        dataset: DataFrame con preguntas (columnas: 'question_title', 'question_content')
        distribution: "uniform" o "exponential"
        lam: parámetro λ para distribución exponencial (media = 1/λ)
        uniform_low, uniform_high: rango para distribución uniforme
        seed: semilla para reproducibilidad
        start_time: datetime inicial para los timestamps (opcional)
        """
        self.dataset = dataset.reset_index(drop=True)
        self.distribution = distribution
        self.lam = lam
        self.uniform_low = uniform_low
        self.uniform_high = uniform_high
        self.rng = np.random.default_rng(seed)
        self.start_time = start_time

    def _sample_interarrival(self):
        """Genera un interarrival basado en la distribución seleccionada"""
        if self.distribution == "uniform":
            return self.rng.integers(self.uniform_low, self.uniform_high)
        elif self.distribution == "exponential":
            if self.lam <= 0:
                raise ValueError("λ debe ser > 0 para distribución exponencial")
            return self.rng.exponential(scale=1.0/self.lam)
        else:
            raise ValueError("Distribución no soportada: usa 'uniform' o 'exponential'")

def check_in_database(db_path, question_index):
    """Verifica si una pregunta ya existe en la base de datos"""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        c.execute("SELECT id FROM queries WHERE question_index=?", (question_index,))
        result = c.fetchone()
        conn.close()
        return result is not None
    except:
        return False

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
    topic_pending = "questions-pending"
    db_path = "/app/data/queries.db"
    
    # Cargar dataset
    df = load_dataset("/app/data/test.csv")
    
    # Crear generador
    gen = TrafficGenerator(df, distribution="uniform", uniform_low=1, uniform_high=5, seed=123)
    
    # Crear productor de Kafka
    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    print("Generador de tráfico iniciado. Enviando preguntas a Kafka...")
    
    # Generar y enviar preguntas
    n_queries = 20
    current_time = 0
    
    for i in range(n_queries):
        # Calcular tiempo de arribo
        inter_arrival = gen._sample_interarrival()
        current_time += inter_arrival
        time.sleep(inter_arrival)
        
        # Elegir aleatoriamente una pregunta
        idx = gen.rng.integers(0, len(gen.dataset))
        row = gen.dataset.iloc[idx]
        
        # Verificar si ya existe en BD
        if check_in_database(db_path, idx):
            print(f"Pregunta {idx} ya existe en BD, omitiendo...")
            continue
        
        # Crear mensaje
        message = {
            "question_index": int(idx),
            "question_title": str(row["question_title"]),
            "question_content": str(row["question_content"]),
            "human_answer": str(row["best_answer"]),
            "timestamp": datetime.now().isoformat(),
            "retry_count": 0
        }
        
        # Enviar a Kafka
        try:
            producer.send(
                topic_pending,
                key=str(idx),
                value=message
            )
            print(f"[{i+1}/{n_queries}] Enviada pregunta {idx}: {row['question_title'][:50]}...")
        except Exception as e:
            print(f"Error enviando pregunta {idx}: {e}")
    
    producer.flush()
    producer.close()
    print("Generación de tráfico completada.")

if __name__ == "__main__":
    main()

