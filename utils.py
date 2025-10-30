# -*- coding: utf-8 -*-
"""Utilidades compartidas para el sistema distribuido"""

import pandas as pd
import google.generativeai as genai
import numpy as np

# Configuración de Gemini
genai.configure(api_key="AIzaSyA5aNjtIegCftM67zsC8ahEgGlEISbcmVc")
model = genai.GenerativeModel("gemini-2.5-flash")
embed_model = "models/embedding-001"

def cosine_similarity(vec1, vec2):
    """Calcula la similitud de coseno entre dos vectores"""
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

def get_embedding(content):
    """Obtiene el embedding de un contenido usando Gemini"""
    return genai.embed_content(model=embed_model, content=str(content))["embedding"]

def calculate_similarity_score(human_answer, llm_answer):
    """Calcula el score de similitud entre respuesta humana y LLM"""
    embedding_humana = get_embedding(human_answer)
    embedding_llm = get_embedding(llm_answer)
    return cosine_similarity(embedding_humana, embedding_llm)

def load_dataset(csv_path="test.csv"):
    """Carga el dataset de preguntas"""
    df = pd.read_csv(
        csv_path,
        header=None,
        quoting=1,
        escapechar="\\",
        on_bad_lines="skip",
        engine="python"
    )
    df = df.rename(columns={
        0: "class_index",
        1: "question_title",
        2: "question_content",
        3: "best_answer"
    })
    return df

def generate_prompt(title, content):
    """Genera el prompt para el LLM basado en título y contenido"""
    if pd.isna(content) or str(content).strip() == "":
        return f"Responde de forma clara y breve a la siguiente pregunta:\n\nTítulo: {title}"
    else:
        return f"Responde de forma clara y breve a la siguiente pregunta:\n\nTítulo: {title}\nPregunta: {content}"

