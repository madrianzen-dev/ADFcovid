# Databricks notebook source
# MAGIC %md
# MAGIC ## Combinación de DataFrames
# MAGIC En Spark, una operación `join()` fusiona filas de DataFrame según las condiciones coincidentes, una operación `crossJoin()` devuelve el producto cartesiano de todas las filas y una operación `union()` concatena DataFrames con esquemas idénticos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links and Resources
# MAGIC - [join](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join)
# MAGIC - [crossjoin](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.crossJoin.html?highlight=join#pyspark.sql.DataFrame.crossJoin)
# MAGIC - [union](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.union.html?highlight=join#pyspark.sql.DataFrame.union)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Definir el esquema para df_1 (Ventas) con transaction_id incluido
schema_sales = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("sales_amount", IntegerType(), True)
])

# Datos para df_1: registros de ventas
# Algunos store_ids (p. ej., 106 y 107) no están presentes en df_2 para demostrar las brechas de unión.
data_sales = [
    (1001, 103, "2025-01-15", 5000),
    (1002, 104, "2025-01-16", 7000),
    (1003, 109, "2025-01-17", 6500),
    (1010, 109, "2025-01-17", 1000),
    (1004, 106, "2025-01-18", 4800),
    (1005, 107, "2025-01-19", 5300)
]

# Crea df_1 con los datos de ventas
df_1 = spark.createDataFrame(data_sales, schema=schema_sales)
df_1.show()

# Define schema for df_2 (Stores)
schema_stores = StructType([
    StructField("id", IntegerType(), True),
    StructField("store_name", StringType(), True),
    StructField("city", StringType(), True)
])

# Definir esquema para df_2 (Tiendas)
data_stores = [
    (101, "Store A", "New York"),
    (102, "Store B", "Los Angeles"),
    (103, "Store C", "Chicago"),
    (104, "Store D", "Houston"),
    (105, "Store E", "Phoenix")
]

# Crea df_2 con los datos de las tiendas
df_2 = spark.createDataFrame(data_stores, schema=schema_stores)
df_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join

# COMMAND ----------

# La unión izquierda devuelve todos los registros del DataFrame izquierdo y solo los registros coincidentes del derecho

left_join_df = df_1.join(df_2, df_1.store_id == df_2.id, "left")

left_join_df.display()

# COMMAND ----------

# La unión derecha devuelve todos los registros del DataFrame izquierdo y solo los registros coincidentes del DataFrame izquierdo.

right_join_df = df_1.join(df_2, df_1.store_id == df_2.id, "right")

right_join_df.display()

# COMMAND ----------

# la unión interna devuelve solo los registros coincidentes de ambos

inner_join_df = df_1.join(df_2, df_1.store_id == df_2.id, "inner")

inner_join_df.display()

# COMMAND ----------

# La unión externa completa devuelve todos los registros del DataFrame izquierdo y derecho, coincidentes o no.

full_join_df = df_1.join(df_2, df_1.store_id == df_2.id, "fullouter")

full_join_df.display()

# COMMAND ----------

# left andi devuelve todos los registros no coincidentes. Solo los registros restantes DataFrame.

left_anti_df = df_1.join(df_2, df_1.store_id == df_2.id, "left_anti")

left_anti_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### crossJoin

# COMMAND ----------

# crossJoin (también conocido como unión cartesiana) devuelve el producto cartesiano de ambos DataFrames

df_1.crossJoin(df_2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union

# COMMAND ----------

# Union agrega dos DataFrames

df_1.union(df_2.selectExpr('*','NULL as campoprueba')).display()
#df_1.display()
#df_2.selectExpr('*','NULL as campoprueba').display()