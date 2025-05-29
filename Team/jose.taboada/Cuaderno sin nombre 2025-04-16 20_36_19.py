# Databricks notebook source
df = spark.read.csv("/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")
df.display()

# COMMAND ----------

spark.sql("""
            Select year(Fecha_Registro) as Mes, count(1)
            from clientes
            where year(Fecha_Registro)='2023'
            group by year(Fecha_Registro)
 
          """).display()