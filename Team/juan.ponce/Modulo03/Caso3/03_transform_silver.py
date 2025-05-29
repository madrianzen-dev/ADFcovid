# Databricks notebook source
from pyspark.sql.functions import col, when

bronze_path = "dbfs:/mnt/ecommerce_etl/bronze/"
silver_path = "dbfs:/mnt/ecommerce_etl/silver/"
rejected_path = "dbfs:/mnt/ecommerce_etl/rejected/"

# Cargar datos de Bronze
df_bronze = spark.read.format("delta").load(bronze_path)

# Validar datos
df_validated = df_bronze.withColumn("monto_total", col("quantity") * col("unit_price"))

# Condiciones inválidas
df_errors = df_validated.filter(
    col("quantity").isNull() | 
    col("unit_price").isNull() |
    col("quantity") <= 0 |
    col("unit_price") <= 0 |
    col("client_name").isNull()
)

# Datos válidos
df_ok = df_validated.subtract(df_errors)

# Guardar resultados
df_ok.write.format("delta").mode("overwrite").save(silver_path)
df_errors.write.format("delta").mode("overwrite").save(rejected_path)

print("Silver y Rejected generados")
