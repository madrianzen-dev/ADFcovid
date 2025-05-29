# Databricks notebook source
from pyspark.sql.functions import month, year, count, sum, avg

silver_path = "dbfs:/mnt/ecommerce_etl/silver/"
gold_path = "dbfs:/mnt/ecommerce_etl/gold/"

df = spark.read.format("delta").load(silver_path)

df_gold = (df
    .withColumn("mes", month("order_date"))
    .withColumn("anio", year("order_date"))
    .groupBy("store", "anio", "mes")
    .agg(
        count("*").alias("num_pedidos"),
        sum("monto_total").alias("total_ventas"),
        avg("monto_total").alias("ticket_promedio")
    )
)

df_gold.write.format("delta").mode("overwrite").save(gold_path)

print("Capa Gold generada")
