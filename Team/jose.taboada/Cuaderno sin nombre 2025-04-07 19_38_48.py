# Databricks notebook source
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", "P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==")
 

# COMMAND ----------

spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", "xfADi7SQ+oQkXWnlgQUAQb91aKPfW8qjdAvk1BLjFvYhr1DSZDS+KY115P5JTaANCXphqHgAW1Q2+AStko91Qw==")

# COMMAND ----------

ruta = "abfss://inputs@dbstoragezat11.dfs.core.windows.net/"
dbutils.fs.ls(ruta)

# COMMAND ----------

df_clientes = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load(ruta + "CL_EMPLEADOS.csv")
df_clientes.display()

# COMMAND ----------

df_clientes = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/customers.csv")
df_clientes.display