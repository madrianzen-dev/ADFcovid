# Databricks notebook source
import os
from pyspark.sql.functions import *
from pyspark.sql import *

from pyspark.sql.functions import col, lit, avg
from pyspark.sql.types import IntegerType, FloatType

# Pregunta 1 : Carga y Exploración Inicial  
path = "abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/energy/owid-energy-data.csv"
df = spark.read.csv(path, header=True, inferSchema=True)
df.show(5)
df.printSchema()

# Pregunta 2 : Limpieza de Datos 
df = df.dropna()

# Pregunta 3 : Transformaciones de Datos 
df = df.withColumn("total_energy", col("primary_energy_consumption") * col("population"))

# Pregunta 4 :  Filtrado de Datos 
df_filtered = df.filter((col("total_energy") > 10000) & (col("year") > 2000))

# Pregunta 5 : Agrupación y Agregación 
df_avg_cont = df_filtered.groupBy("country").agg(avg("energy_per_capita").alias("avg_energy_per_capita"))
df_avg_cont.show()

# Pregunta 6 : Ordenamiento de Datos  "por toda la base" 
df.orderBy(col("energy_per_capita").desc()).select("country", "energy_per_capita").show(10)

# Pregunta 7 : Funciones de Ventana 
df = df.dropDuplicates(["country", "year"])

# Pregunta 8 :  Conversión de Tipos de Datos
df = df.withColumn("year", col("year").cast(IntegerType()))
df = df.withColumn("primary_energy_consumption", col("primary_energy_consumption").cast(FloatType()))

#  Guardado de Resultados :
output_path = "abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/raw/energy"
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.sql("""
CREATE TABLE IF NOT EXISTS zatdev11db.bronze.energy_hrc
USING DELTA
LOCATION 'abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/output/energy'
""")
