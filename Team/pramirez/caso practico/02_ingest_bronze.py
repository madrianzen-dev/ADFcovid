# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_date
from pyspark.sql.types import *

input_path = "/mnt/ecommerce_etl/input/"
bronze_path = "/mnt/ecommerce_etl/bronze/"

# Leer archivos CSV
df_raw = spark.read.option("header", "true").csv(input_path)

# Agregar columnas de metadatos
df_bronze = df_raw.withColumn("ingestion_date", current_date()) \
                  .withColumn("archivo_origen", input_file_name())

# Guardar en formato Delta
df_bronze.write.mode("overwrite").format("delta").save(bronze_path)
