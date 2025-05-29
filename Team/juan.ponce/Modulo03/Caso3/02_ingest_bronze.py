# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp

input_path = "dbfs:/mnt/ecommerce_etl/input/"
bronze_path = "dbfs:/mnt/ecommerce_etl/bronze/"

# Leer todos los archivos CSV
df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("archivo_origen", input_file_name())
)

# Guardar como Delta
df.write.format("delta").mode("append").save(bronze_path)

print("Datos cargados en capa Bronze")
