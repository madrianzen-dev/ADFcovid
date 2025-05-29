# Databricks notebook source
# MAGIC %sql
# MAGIC select "hola mundo Databrick" from  zatdev111db.bronze.customers_hrc limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zatdev111db.bronze.hola_mundo_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zatdev111db.bronze.caso_stock_audit_hrc limit 10;

# COMMAND ----------

df = spark.read.table("zatdev111db.silver.caso_stock_positivo_hrc")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM zatdev111db.system.event_log
# MAGIC WHERE pipeline_id = 'b7fb67d8-2a4a-474e-845f-868dfb88bcba'
# MAGIC   AND event_type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC;
# MAGIC

# COMMAND ----------

dbutils.fs.cp(
    "file:/tmp/loans_parquet/loans.parquet", 
    "dbfs:/tmp/delta_demo/loans_backup/", 
    recurse=True
)

# COMMAND ----------

import shutil

# Copiar manualmente del filesystem local hacia DBFS
shutil.copy(
    "/dbfs/tmp/loans_parquet/loans.parquet", 
    "/dbfs/tmp/delta_demo/loans_backup/loans.parquet"
)

# COMMAND ----------

import shutil
dbutils.fs.cp("file:/tmp/loans_parquet/loans.parquet", "/tmp/delta_demo/loans_backup/loans.parquet")
#df = spark.read.parquet("file:/tmp/loans_parquet/loans.parquet")
#df.show()
# Escribirlo en DBFS
#df.write.mode("overwrite").parquet("dbfs:/tmp/delta_demo/loans_backup/loans.parquet")

# COMMAND ----------

df = spark.read.parquet("file:/tmp/loans_parquet/loans.parquet")
#df.display()



# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/hrc/app_20250421.csv", header=True, inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

import os

print(os.listdir("/tmp/"))

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet/; wget -O /dbfs/tmp/delta_demo/loans_parquet/loans.parquet https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet
# MAGIC  

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet_2/; wget -O /dbfs/tmp/delta_demo/loans_parquet_2/loans.parquet https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet

# COMMAND ----------

import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
#df = spark.read.format("parquet").load("file:/dbfs/tmp/delta_demo/loans_parquet/")

parquet_path = "file:/dbfs/tmp/delta_demo/loans_parquet_2/"
df = (spark.read.format("parquet").load(parquet_path)
      .withColumn("type",lit("batch"))
      .withColumn("timestamp",current_timestamp()))

#Lo vamos a escribir como formato delta
df.write.format("delta").mode("overwrite").saveAsTable("loans_delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

dbutils.fs.rm("dbfs:/dbfs:", True)

# COMMAND ----------

GRANT ALL PRIVILEGES ON SCHEMA default TO `zat.pe`;


# COMMAND ----------

spark.sql("SHOW GRANTS ON USER 'Heberto Rosillo'")