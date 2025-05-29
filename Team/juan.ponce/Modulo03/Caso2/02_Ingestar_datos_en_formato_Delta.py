# Databricks notebook source
from pyspark.sql.functions import input_file_name, current_timestamp

ruta_input="dbfs:/mnt/etl_pedidos_jp/input/"
ruta_bronze="dbfs:/mnt/etl_pedidos_jp/bronze/"
ruta_checkpoint="dbfs:/mnt/etl_pedidos_jp/checkpoint/bronze/"

df_stream = (spark.readStream
    .option("header","true")
    .schema("id_pedido STRING, cliente STRING, producto STRING, cantidad INT, precio_unitario DOUBLE, fecha_pedido STRING")
    .csv(ruta_input)
    .withColumn("llegada",current_timestamp())
    .withColumn("archivo_fuente",input_file_name())
)

(df_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", ruta_checkpoint)
    .trigger(once=True)
    .start(ruta_bronze)
    .awaitTermination()
    )
