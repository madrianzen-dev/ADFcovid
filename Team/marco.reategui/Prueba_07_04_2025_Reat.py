# Databricks notebook source
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", "P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==")

# COMMAND ----------

ruta = "abfss://inputs@dbstoragezat11.dfs.core.windows.net/"
dbutils.fs.ls(ruta)

# COMMAND ----------

df_clientes =spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load(ruta + "CL_EMPLEADOS.csv")
df_clientes.display()

# COMMAND ----------

df_clientes.write.format("delta").mode("overwrite").save(ruta + "CL_EMPLEADOS_DELTA")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists zatdev11db.silver.customers_reat;  
# MAGIC create or replace table zatdev11db.silver.customers_reat(
# MAGIC  customer_id int,
# MAGIC  name string,
# MAGIC  email string,
# MAGIC  silver_load_ts timestamp
# MAGIC )
# MAGIC  USING DELTA
# MAGIC  LOCATION 'abfss://data@dbstoragezat11.dfs.core.windows.net/silver/customers_reat';

# COMMAND ----------

df_clientes = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/customers.csv")
df_clientes.display()

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp
df_clientes_bronze = df_clientes.withColumn("bronze_load_ts", current_timestamp())
df_clientes_bronze.display()
df_clientes_bronze.write.format("delta").mode("overwrite").saveAsTable("zatdev111db.bronze.customers_reat")