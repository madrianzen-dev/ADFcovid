# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType

bronze_path = "/mnt/ecommerce_etl/bronze/"
silver_path = "/mnt/ecommerce_etl/silver/"
rejected_path = "/mnt/ecommerce_etl/rejected/"

# Leer datos de bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# Convertir tipos para validaciones
df_casted = df_bronze.withColumn("quantity", col("quantity").cast(IntegerType())) \
                     .withColumn("unit_price", col("unit_price").cast(DoubleType()))

# Validaciones
df_valid = df_casted.filter(
    (col("quantity").isNotNull()) &
    (col("unit_price").isNotNull()) &
    (col("quantity") > 0) & (col("quantity") <= 100) &
    (col("unit_price") > 0)
)

df_invalid = df_casted.subtract(df_valid)

# Guardar
df_valid.write.mode("overwrite").format("delta").save(silver_path)
df_invalid.write.mode("overwrite").format("delta").save(rejected_path)
