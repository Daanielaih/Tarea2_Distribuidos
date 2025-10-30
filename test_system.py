# -*- coding: utf-8 -*-
"""Script de prueba para verificar el sistema"""

import sqlite3
import pandas as pd
import os

db_path = "data/queries.db"

if not os.path.exists(db_path):
    print("❌ La base de datos no existe aún. Ejecuta el sistema primero.")
    exit(1)

conn = sqlite3.connect(db_path)

# Verificar que la tabla existe
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='queries'")
if not cursor.fetchone():
    print("❌ La tabla 'queries' no existe.")
    conn.close()
    exit(1)

# Obtener estadísticas
df = pd.read_sql("SELECT * FROM queries", conn)

print("\n=== ESTADÍSTICAS DEL SISTEMA ===\n")
print(f"Total de preguntas procesadas: {len(df)}")
print(f"\nScore promedio: {df['similarity_score'].mean():.4f}")
print(f"Score mínimo: {df['similarity_score'].min():.4f}")
print(f"Score máximo: {df['similarity_score'].max():.4f}")
print(f"\nTotal de reintentos (count > 1): {(df['count'] > 1).sum()}")

if len(df) > 0:
    print("\n=== ÚLTIMAS 5 PREGUNTAS ===\n")
    print(df[['question_index', 'similarity_score', 'count']].tail(5).to_string(index=False))

conn.close()
print("\n✅ Verificación completada.")

