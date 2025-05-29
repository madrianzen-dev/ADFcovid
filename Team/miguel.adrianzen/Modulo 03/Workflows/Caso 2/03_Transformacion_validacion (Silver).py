# Databricks notebook source
from pyspark.sql.functions import col, when

ruta_bronze="dbfs:/mnt/etl_pedidos/bronze/"
ruta_silver="dbfs:/mnt/etl_pedidos/silver/"
ruta_rejected="dbfs:/mnt/etl_pedidos/rejected/"

df = spark.read.format("delta").load(ruta_bronze)

#Reglas de validación:
df_validado = df.withColumn("monto_total",col("cantidad")*col("precio_unitario"))

df_errores = df_validado.filter(
    (col("cantidad")  <= 0) |
    (col("precio_unitario") <= 0) |
    (col("cliente").isNull())
)

df_ok = df_validado.subtract(df_errores)
df_ok.write.format("delta").mode("overwrite").save(ruta_silver)
df_errores.write.format("delta").mode("overwrite").save(ruta_rejected)