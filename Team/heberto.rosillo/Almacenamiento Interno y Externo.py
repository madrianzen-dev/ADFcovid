# Databricks notebook source
dbutils.fs.ls("dbfs:/")

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/FileStore/SAISEU19_loan_risks_snappy.parquet")
df.inputFiles()

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/tmp/casoventasCorp_hrc")
print("Ejemplo de archivo:")
print(files[0].path)

# Intentar leer inputFile para ver si hay abfss:// o s3a://
from pyspark.sql import SparkSession
df = spark.read.text(files[0].path)
print(df.inputFiles())