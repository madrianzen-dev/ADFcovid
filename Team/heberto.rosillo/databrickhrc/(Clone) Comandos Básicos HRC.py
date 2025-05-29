# Databricks notebook source
# MAGIC %sql
# MAGIC select "hola mundo Databrick" from  zatdev111db.bronze.customers_hrc limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zatdev111db.bronze.hola_mundo_view;

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inicializar sesión de Spark
spark = SparkSession.builder \
    .appName("Ejemplo DataFrame Dummy") \
    .getOrCreate()

# Definir datos
data = [
    ("Ana", 23, "Madrid"),
    ("Luis", 34, "Barcelona"),
    ("María", 29, "Sevilla"),
    ("Carlos", 45, "Valencia"),
    ("Sofía", 31, "Bilbao")
]

# Definir esquema (schema)
schema = StructType([
    StructField("Nombre", StringType(), True),
    StructField("Edad", IntegerType(), True),
    StructField("Ciudad", StringType(), True)
])

# Crear el DataFrame
df = spark.createDataFrame(data, schema)

# Mostrar el DataFrame
df.show()

# COMMAND ----------

#####hola mundo