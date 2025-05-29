# Databricks notebook source
#spark SQL
#Crear una vista temporal
df=spark.read.csv("/FileStore/tables/caso_ventasCorp.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("ventas")

# COMMAND ----------

#Consulta
spark.sql("select * from ventas limit 5").display()