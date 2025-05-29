# Databricks notebook source
from pyspark.sql.functions import col, month, year, sum, count, avg

silver_path = "/mnt/ecommerce_etl/silver/"
gold_path = "/mnt/ecommerce_etl/gold/"

# Leer datos limpios
df_silver = spark.read.format("delta").load(silver_path)

# Agregar columnas de mes y año
df_enriched = df_silver.withColumn("order_month", month("order_date")) \
                       .withColumn("order_year", year("order_date")) \
                       .withColumn("total_venta", col("quantity") * col("unit_price"))

# Agregaciones
df_gold = df_enriched.groupBy("store", "order_year", "order_month") \
    .agg(
        sum("total_venta").alias("total_ventas"),
        count("order_id").alias("numero_pedidos"),
        avg("total_venta").alias("ticket_promedio")
    )

# Guardar resultado
df_gold.write.mode("overwrite").format("delta").save(gold_path)
