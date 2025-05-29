# Databricks notebook source
import dlt
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# Esquema
schema = StructType([
    StructField("id_producto", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("estado", StringType(), True),
    StructField("canal", StringType(), True)
])

@dlt.table(
  name="caso_stock_audit_hrc",  # Nombre de la tabla en bronze
  comment="Tabla Bronze: auditoría de stock productos HRC",
  table_properties={
    "quality": "bronze",    # Marca calidad en propiedades
    "pipelines.reset.allowed": "true"  # Opcional: permitir reset de pipeline
  }
)
def load_caso_stock_audit_hrc():
    df_csv = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(schema)
        .load("./FileStore/hrc/*20250421.csv")
        .withColumn("llegada", current_timestamp())
    )
    return df_csv

# COMMAND ----------

@dlt.table(
  name="silver.caso_stock_positivo_hrc",
  comment="Tabla Silver: auditoría de stock con productos mayores a cero",
  table_properties={"quality": "silver",
                    "pipelines.reset.allowed": "true"
                    }
)
@dlt.expect_or_drop("stock_positivo", "stock > 0")
def load_caso_stock_hrc_stock_positivo():
    df = dlt.read("caso_stock_audit_hrc")
    return df