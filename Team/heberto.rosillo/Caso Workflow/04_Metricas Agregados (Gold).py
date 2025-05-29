# Databricks notebook source

from pyspark.sql.functions import col, month, year, sum as _sum, count, round

ruta_silver = "dbfs:/mnt/ecommerce_etl/silver_hrc/"
ruta_gold = "dbfs:/mnt/ecommerce_etl/gold_hrc/"

df = spark.read.format("delta").load(ruta_silver)


df_fecha = df.withColumn("mes", month("fecha_pedido")).withColumn("anio", year("fecha_pedido"))


df_agg = df_fecha.groupBy("cliente", "anio", "mes").agg(
    _sum("monto_total").alias("total_ventas"),
    count("*").alias("num_pedidos")
).withColumn(
    "ticket_promedio", round(col("total_ventas") / col("num_pedidos"), 2)
)

df_agg.write.format("delta").mode("overwrite").save(ruta_gold)
