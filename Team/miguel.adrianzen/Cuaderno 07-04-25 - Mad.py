# Databricks notebook source
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", "xfADi7SQ+oQkXWnlgQUAQb91aKPfW8qjdAvk1BLjFvYhr1DSZDS+KY115P5JTaANCXphqHgAW1Q2+AStko91Qw==")

# COMMAND ----------

ruta = "abfss://inputs@dbstoragezat11.dfs.core.windows.net/"
dbutils.fs.ls(ruta)

# COMMAND ----------

df_clientes = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/customers.csv") 
df_clientes.display()

# COMMAND ----------

df_clientes.write.format("delta").mode("overwrite").save("default.CL_EMPLEADOS_tb")

# COMMAND ----------

df_clientes.write.format("delta").mode("overwrite").save(ruta + "CL_EMPLEADOS_TB_mad")
 

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists zatdev111db managed location 'abfss://data@dbstoragezat11.dfs.core.windows.net/';
# MAGIC create schema  if not exists zatdev111db.bronze;
# MAGIC create schema  if not exists zatdev111db.silver;
# MAGIC create schema  if not exists zatdev111db.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table zatdev11db.silver.customers_mad (
# MAGIC  customer_id int,
# MAGIC  name string,
# MAGIC  email string,
# MAGIC  silver_load_ts timestamp
# MAGIC )
# MAGIC  USING DELTA
# MAGIC  LOCATION 'abfss://data@dbstoragezat11.dfs.core.windows.net/silver/customers_mad';

# COMMAND ----------

df_clientes = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/customers.csv") 
df_clientes.show()

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp
df_clientes_bronze = df_clientes.withColumn("bronze_load_ts", current_timestamp())
df_clientes_bronze.display()
df_clientes_bronze.write.mode("overwrite").saveAsTable("zatdev111db.bronze.customers_mad_parquet")