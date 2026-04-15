# 🌊 Análisis de Calidad del Agua en Colombia con Apache Spark y Kafka

## 📌 Descripción del Proyecto
Este proyecto implementa una solución de Big Data para el análisis de la calidad del agua en Colombia, utilizando datos oficiales del IDEAM. Incluye procesamiento batch con Apache Spark y procesamiento en tiempo real con Kafka + Spark Streaming.

## 🎯 Problema Identificado
Monitoreo de contaminación por metales pesados (Mercurio, Plomo, Cadmio) y parámetros fisicoquímicos en fuentes hídricas colombianas, con generación de alertas en tiempo real.

## 📊 Dataset
- **Fuente:** IDEAM - Datos Abiertos Colombia
- **Variables:** Metales pesados, parámetros fisicoquímicos, microbiológicos
- **Período:** 2005 - presente
- **URL:** https://www.datos.gov.co/Ambiente-y-Desarrollo-Sostenible/Data-hist-rica-de-calidad-de-agua/62gv-3857

## 🏗️ Arquitectura

| Componente | Tecnología | Función |
|------------|------------|---------|
| Mensajería | Apache Kafka | Transporte de datos en tiempo real |
| Procesamiento Batch | Apache Spark | Análisis histórico |
| Procesamiento Streaming | Apache Spark Streaming | Alertas en tiempo real |
| Orquestación | ZooKeeper | Coordinación de Kafka |

## 📋 Scripts del Proyecto

| Archivo | Descripción |
|---------|-------------|
| `batch_analisis_agua.py` | Análisis batch: estadísticas de metales pesados por departamento |
| `productor_agua.py` | Simula envío de mediciones de calidad del agua a Kafka |
| `streaming_agua.py` | Consume datos de Kafka y genera alertas en tiempo real |

## 🚀 Instalación y Ejecución

### Requisitos Previos
- Ubuntu 24.04 (o máquina virtual con Spark y Kafka instalados)
- Python 3.12
- Apache Spark 3.5.x
- Apache Kafka 3.9.x

### Instalar dependencias Python
```bash
pip install kafka-python pyspark
