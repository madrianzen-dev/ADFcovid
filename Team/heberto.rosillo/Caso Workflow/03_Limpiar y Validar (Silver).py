# Databricks notebook source
from pyspark.sql.functions import col, when

ruta_bronze="dbfs:/mnt/ecommerce_etl/bronze_hrc/"
ruta_silver="dbfs:/mnt/ecommerce_etl/silver_hrc/"
ruta_rejected="dbfs:/mnt/ecommerce_etl/rejected_hrc/"

df = spark.read.format("delta").load(ruta_bronze)

#Reglas de validación:
df_validado = df.withColumn("monto_total",col("cantidad")*col("precio_unitario"))

df_errores = df_validado.filter(
    (col("cantidad") <= 0) |
    (col("cantidad") > 100) |
    (col("precio_unitario") <= 0) |
    (col("cliente").isNull())
)

df_ok = df_validado.subtract(df_errores)
df_ok.write.format("delta").mode("overwrite").save(ruta_silver)
df_errores.write.format("delta").mode("overwrite").save(ruta_rejected)

