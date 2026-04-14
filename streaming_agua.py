#!/usr/bin/env python3
# streaming_agua.py - Procesamiento streaming de calidad del agua

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

print("="*60)
print("🌊 STREAMING - MONITOREO DE CALIDAD DEL AGUA EN TIEMPO REAL")
print("="*60)

spark = SparkSession.builder.appName("CalidadAgua_Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("departamento", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("propiedad_observada", StringType(), True),
    StructField("resultado", StringType(), True),
    StructField("unidad", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("fuente", StringType(), True)
])

def limpiar_valor(valor):
    if valor is None: return None
    if isinstance(valor, str) and valor.startswith('<'): return 0.0
    try: return float(valor)
    except: return None

from pyspark.sql.functions import udf
limpiar_udf = udf(limpiar_valor, DoubleType())

df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "agua_calidad") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
df_numerico = df_parsed.withColumn("valor_numerico", limpiar_udf(col("resultado")))

# Alertas de Mercurio (>0.1 mg/kg)
alertas_mercurio = df_numerico.filter((col("propiedad_observada").contains("MERCURIO")) & (col("valor_numerico") > 0.1)) \
    .select("departamento", "municipio", "propiedad_observada", "resultado", "unidad", "timestamp")

# Estadísticas por departamento (ventana 15 segundos)
estadisticas_depto = df_numerico.withWatermark("timestamp", "15 seconds") \
    .groupBy(window(col("timestamp"), "15 seconds", "5 seconds"), col("departamento")) \
    .agg(count("*").alias("mediciones_recibidas"), avg("valor_numerico").alias("promedio_general")) \
    .filter(col("mediciones_recibidas") > 0)

print("\n📺 Mostrando alertas y estadísticas en tiempo real...\n")

query1 = alertas_mercurio.writeStream.outputMode("append").format("console").option("truncate", "false").queryName("⚠️ ALERTAS DE MERCURIO (>0.1 mg/kg)").start()
query2 = estadisticas_depto.writeStream.outputMode("append").format("console").option("truncate", "false").queryName("📊 ESTADÍSTICAS POR DEPARTAMENTO").start()

print("✅ Streaming activo. Esperando datos...")
print("Presiona Ctrl+C para detener\n")
query1.awaitTermination()