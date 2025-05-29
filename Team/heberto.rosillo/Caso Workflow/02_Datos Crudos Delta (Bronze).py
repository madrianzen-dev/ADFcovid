# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp

ruta_input="dbfs:/mnt/ecommerce_etl/input_hrc/"
ruta_bronze="dbfs:/mnt/ecommerce_etl/bronze_hrc/"
ruta_checkpoint="dbfs:/mnt/ecommerce_etl/checkpoint/bronze_hrc/"

df_stream = (spark.readStream
    .option("header","true")
    .schema("id_pedido STRING, cliente STRING, producto STRING, cantidad INT, precio_unitario DOUBLE, fecha_pedido STRING")
    .csv(ruta_input)
    .withColumn("ingestion_date",current_timestamp())
    .withColumn("archivo_origen",input_file_name())
)

(df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(once=True)
    .start(ruta_bronze)
    .awaitTermination()
    )
