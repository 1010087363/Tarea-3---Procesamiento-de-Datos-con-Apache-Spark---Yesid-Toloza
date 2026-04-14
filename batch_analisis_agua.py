#!/usr/bin/env python3
# batch_analisis_agua.py - Análisis batch de calidad del agua IDEAM Colombia

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("="*70)
print("🌊 ANÁLISIS BATCH - CALIDAD DEL AGUA EN COLOMBIA (IDEAM)")
print("="*70)

# Crear sesión Spark
spark = SparkSession.builder.appName("CalidadAgua_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Datos de ejemplo basados en registros reales del IDEAM
from pyspark.sql import Row

datos_ejemplo = [
    Row(departamento="CALDAS", municipio="AGUADAS", propiedad_observada="MERCURIO TOTAL EN SEDIMENTOS", resultado="0.087", unidad="mg Hg/kg", fecha="2007-03-09"),
    Row(departamento="CALDAS", municipio="AGUADAS", propiedad_observada="MERCURIO TOTAL EN SEDIMENTOS", resultado="0.073", unidad="mg Hg/kg", fecha="2011-04-11"),
    Row(departamento="CALDAS", municipio="AGUADAS", propiedad_observada="MERCURIO TOTAL EN SEDIMENTOS", resultado="0.092", unidad="mg Hg/kg", fecha="2016-11-07"),
    Row(departamento="HUILA", municipio="NEIVA", propiedad_observada="MERCURIO TOTAL EN SEDIMENTOS", resultado="0.224", unidad="mg Hg/kg", fecha="2010-09-24"),
    Row(departamento="HUILA", municipio="NEIVA", propiedad_observada="MERCURIO TOTAL EN SEDIMENTOS", resultado="0.054", unidad="mg Hg/kg", fecha="2010-12-09"),
    Row(departamento="HUILA", municipio="NEIVA", propiedad_observada="PLOMO TOTAL EN AGUA", resultado="0.05", unidad="mg Pb/L", fecha="2007-03-09"),
    Row(departamento="VALLE", municipio="CALI", propiedad_observada="CADMIO TOTAL EN AGUA", resultado="<0.01", unidad="mg Cd/L", fecha="2007-03-09"),
    Row(departamento="ANTIOQUIA", municipio="MEDELLIN", propiedad_observada="COLIFORMES TOTALES", resultado="18000", unidad="UFC/100 mL", fecha="2007-03-09"),
]

df_raw = spark.createDataFrame(datos_ejemplo)

# Función para limpiar valores con "<"
def limpiar_resultado(valor):
    if valor is None:
        return None
    if isinstance(valor, str) and valor.startswith('<'):
        return 0.0
    try:
        return float(valor)
    except:
        return None

from pyspark.sql.functions import udf
limpiar_udf = udf(limpiar_resultado, DoubleType())
df_clean = df_raw.withColumn("resultado_numerico", limpiar_udf(col("resultado")))
df_validos = df_clean.filter(col("resultado_numerico").isNotNull())

# Análisis de Mercurio
print("\n⚠️ ANÁLISIS DE MERCURIO (Hg) - CONTAMINANTE CRÍTICO")
df_mercurio = df_validos.filter(col("propiedad_observada").contains("MERCURIO"))

if df_mercurio.count() > 0:
    print("\n📌 NIVELES DE MERCURIO POR DEPARTAMENTO:")
    df_mercurio.groupBy("departamento").agg(
        avg("resultado_numerico").alias("promedio_Hg_mg_kg"),
        max("resultado_numerico").alias("max_Hg_mg_kg"),
        count("*").alias("num_muestras")
    ).orderBy(desc("promedio_Hg_mg_kg")).show(10, truncate=False)

# Resumen final
print("\n" + "="*70)
print("🎉 ANÁLISIS BATCH COMPLETADO")
print("="*70)
print(f"\n📊 Total de registros analizados: {df_validos.count()}")
print(f"📊 Departamentos con datos: {df_validos.select('departamento').distinct().count()}")