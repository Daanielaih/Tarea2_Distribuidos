#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script de pruebas completas del sistema"""

import os
import sys
import json

def test_file_structure():
    """Verifica que todos los archivos necesarios existan"""
    print("=== PRUEBA 1: Estructura de archivos ===\n")
    
    required_files = [
        "utils.py",
        "docker-compose.yml",
        "requirements.txt",
        "README.md",
        "EXPLICACION.txt",
        "services/traffic_generator/main.py",
        "services/traffic_generator/Dockerfile",
        "services/llm_consumer/main.py",
        "services/llm_consumer/Dockerfile",
        "services/rate_limit_retry/main.py",
        "services/rate_limit_retry/Dockerfile",
        "services/overload_retry/main.py",
        "services/overload_retry/Dockerfile",
        "services/storage_consumer/main.py",
        "services/storage_consumer/Dockerfile",
        "flink/flink_job.py",
        "flink/Dockerfile",
        "data/test.csv"
    ]
    
    all_ok = True
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✓ {file_path}")
        else:
            print(f"✗ {file_path} NO EXISTE")
            all_ok = False
    
    print(f"\nResultado: {'✓ TODOS LOS ARCHIVOS PRESENTES' if all_ok else '✗ FALTAN ARCHIVOS'}\n")
    return all_ok

def test_python_syntax():
    """Verifica sintaxis de Python"""
    print("=== PRUEBA 2: Sintaxis Python ===\n")
    
    python_files = [
        "utils.py",
        "services/traffic_generator/main.py",
        "services/llm_consumer/main.py",
        "services/rate_limit_retry/main.py",
        "services/overload_retry/main.py",
        "services/storage_consumer/main.py",
        "flink/flink_job.py",
        "test_system.py"
    ]
    
    all_ok = True
    for file_path in python_files:
        if not os.path.exists(file_path):
            continue
        try:
            with open(file_path, 'r') as f:
                compile(f.read(), file_path, 'exec')
            print(f"✓ {file_path}")
        except SyntaxError as e:
            print(f"✗ {file_path}: {e}")
            all_ok = False
    
    print(f"\nResultado: {'✓ SINTAXIS CORRECTA' if all_ok else '✗ ERRORES DE SINTAXIS'}\n")
    return all_ok

def test_docker_compose():
    """Verifica docker-compose.yml"""
    print("=== PRUEBA 3: Docker Compose ===\n")
    
    try:
        with open("docker-compose.yml", 'r') as f:
            content = f.read()
        
        # Verificar servicios esenciales
        required_services = [
            "zookeeper",
            "kafka",
            "traffic-generator",
            "llm-consumer",
            "rate-limit-retry",
            "overload-retry",
            "flink-score-processor",
            "storage-consumer"
        ]
        
        all_ok = True
        for service in required_services:
            if f"{service}:" in content:
                print(f"✓ Servicio {service} definido")
            else:
                print(f"✗ Servicio {service} NO encontrado")
                all_ok = False
        
        # Verificar red
        if "kafka-network:" in content:
            print("✓ Red kafka-network definida")
        else:
            print("✗ Red kafka-network NO encontrada")
            all_ok = False
        
        print(f"\nResultado: {'✓ DOCKER COMPOSE OK' if all_ok else '✗ PROBLEMAS EN DOCKER COMPOSE'}\n")
        return all_ok
        
    except Exception as e:
        print(f"✗ Error leyendo docker-compose.yml: {e}\n")
        return False

def test_imports():
    """Verifica imports críticos"""
    print("=== PRUEBA 4: Imports críticos ===\n")
    
    # Verificar que utils.py tiene las funciones necesarias
    try:
        with open("utils.py", 'r') as f:
            utils_content = f.read()
        
        required_functions = [
            "cosine_similarity",
            "get_embedding",
            "calculate_similarity_score",
            "load_dataset",
            "generate_prompt"
        ]
        
        all_ok = True
        for func in required_functions:
            if f"def {func}" in utils_content:
                print(f"✓ Función {func} presente")
            else:
                print(f"✗ Función {func} NO encontrada")
                all_ok = False
        
        # Verificar imports en servicios
        services_to_check = [
            ("services/llm_consumer/main.py", ["kafka", "json", "google.generativeai"]),
            ("services/traffic_generator/main.py", ["kafka", "pandas", "numpy"]),
            ("flink/flink_job.py", ["kafka", "json", "utils"]),
        ]
        
        for file_path, imports in services_to_check:
            if not os.path.exists(file_path):
                continue
            with open(file_path, 'r') as f:
                content = f.read()
            for imp in imports:
                if imp in content.lower() or f"import {imp}" in content or f"from {imp}" in content:
                    print(f"✓ {file_path}: import {imp} OK")
                else:
                    print(f"⚠ {file_path}: import {imp} puede faltar")
        
        print(f"\nResultado: {'✓ IMPORTS OK' if all_ok else '✗ PROBLEMAS CON IMPORTS'}\n")
        return all_ok
        
    except Exception as e:
        print(f"✗ Error verificando imports: {e}\n")
        return False

def test_dataset():
    """Verifica que el dataset existe y tiene formato correcto"""
    print("=== PRUEBA 5: Dataset ===\n")
    
    try:
        if not os.path.exists("data/test.csv"):
            print("✗ data/test.csv NO existe")
            return False
        
        with open("data/test.csv", 'r') as f:
            lines = f.readlines()
        
        if len(lines) < 2:
            print(f"✗ Dataset tiene muy pocas líneas: {len(lines)}")
            return False
        
        print(f"✓ Dataset tiene {len(lines)} líneas")
        
        # Verificar que las primeras líneas tienen formato esperado
        if len(lines[0].split(',')) >= 3:
            print("✓ Formato CSV parece correcto")
        else:
            print("⚠ Formato CSV puede tener problemas")
        
        print("\nResultado: ✓ DATASET OK\n")
        return True
        
    except Exception as e:
        print(f"✗ Error verificando dataset: {e}\n")
        return False

def test_kafka_topics():
    """Verifica que se mencionan los tópicos correctos"""
    print("=== PRUEBA 6: Tópicos de Kafka ===\n")
    
    required_topics = [
        "questions-pending",
        "llm-responses-success",
        "llm-errors-rate-limit",
        "llm-errors-overload",
        "results-valid"
    ]
    
    all_ok = True
    for topic in required_topics:
        found = False
        for root, dirs, files in os.walk("services"):
            for file in files:
                if file.endswith(".py"):
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, 'r') as f:
                            if topic in f.read():
                                found = True
                                break
                    except:
                        pass
                if found:
                    break
            if found:
                break
        
        # También verificar flink
        if not found:
            try:
                with open("flink/flink_job.py", 'r') as f:
                    if topic in f.read():
                        found = True
            except:
                pass
        
        if found:
            print(f"✓ Tópico '{topic}' encontrado")
        else:
            print(f"✗ Tópico '{topic}' NO encontrado")
            all_ok = False
    
    print(f"\nResultado: {'✓ TÓPICOS OK' if all_ok else '✗ FALTAN TÓPICOS'}\n")
    return all_ok

def main():
    print("=" * 60)
    print("PRUEBAS COMPLETAS DEL SISTEMA DISTRIBUIDO")
    print("=" * 60)
    print()
    
    tests = [
        ("Estructura de archivos", test_file_structure),
        ("Sintaxis Python", test_python_syntax),
        ("Docker Compose", test_docker_compose),
        ("Imports críticos", test_imports),
        ("Dataset", test_dataset),
        ("Tópicos Kafka", test_kafka_topics)
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ Error en prueba '{name}': {e}\n")
            results.append((name, False))
    
    print("=" * 60)
    print("RESUMEN DE PRUEBAS")
    print("=" * 60)
    print()
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")
    
    print()
    print(f"Total: {passed}/{total} pruebas pasadas")
    
    if passed == total:
        print("\n🎉 TODAS LAS PRUEBAS PASARON")
        return 0
    else:
        print(f"\n⚠ {total - passed} PRUEBA(S) FALLARON")
        return 1

if __name__ == "__main__":
    sys.exit(main())

