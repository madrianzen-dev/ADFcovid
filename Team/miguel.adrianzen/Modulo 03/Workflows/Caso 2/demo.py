# Databricks notebook source
from pyspark.sql.types import StructType, StringType, IntegerType

csv_path = "/mnt/my_data_stream"
chk_path = "/mnt/checkpoints/stream1"
out_path = "/mnt/output"

# Limpiar si ya existe
dbutils.fs.rm(csv_path, recurse=True)
dbutils.fs.mkdirs(csv_path)

dbutils.fs.rm(chk_path, recurse=True)
dbutils.fs.mkdirs(chk_path)

dbutils.fs.rm(out_path, recurse=True)
dbutils.fs.mkdirs(out_path)

schema = StructType() \
    .add("name", StringType()) \
    .add("age", IntegerType())

stream_df = spark.readStream \
    .option("header", "true") \
    .option("sep", ",") \
    .schema(schema) \
    .csv(csv_path)  # Carpeta donde se agregan archivos nuevos

# COMMAND ----------

query = stream_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/checkpoints/stream1") \
    .option("path", "/mnt/output") \
    .outputMode("append") \
    .start()

# COMMAND ----------

# Copiar archivo subido desde FileStore
dbutils.fs.cp("dbfs:/FileStore/ar2.csv", "/mnt/my_data_stream/ar2.csv")

# COMMAND ----------

# Lectura por lotes
df = spark.read.parquet("/mnt/output")
display(df)
query.stop()