# Databricks notebook source
from pyspark.sql.functions import avg

#Leer la tabla creada en el paso anterior
df = spark.table("temp_users_raw")

#Transformar: Calculamos el promedio por ciudad
result = df.groupBy("city").agg(avg("age").alias("promedio_edad"))

#Guardamos el resultado
result.write.mode("overwrite").saveAsTable("temp_users_promedio")