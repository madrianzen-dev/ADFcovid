# Databricks notebook source
# MAGIC %md
# MAGIC ## Modos de escritura
# MAGIC - El modo `Sobrescribir` reemplaza completamente los datos existentes en la ubicación de destino con el nuevo conjunto de datos, eliminando así todos los registros anteriores.
# MAGIC - El modo `Anexar` añade nuevos registros al conjunto de datos existente sin alterar los datos actuales, siempre que el esquema de los nuevos datos sea compatible.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links and Resources
# MAGIC - [DataFrameWriter Mode Method](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html)

# COMMAND ----------

# Lectura de los datos de Countries_Parquet en un DataFrame (df)
df = spark.read.format("parquet").load("abfss://data@dbstoragezat11.dfs.core.windows.net/parquet/countries_parquet")

# COMMAND ----------

# There are 195 rows
df.count()

# COMMAND ----------

# Overwrite mode overwrites existing data, hence why the count is again 195
df.write.mode("overwrite").format("parquet").save("abfss://data@dbstoragezat11.dfs.core.windows.net/parquet/countries_parquet")
df = spark.read.format("parquet").load("abfss://data@dbstoragezat11.dfs.core.windows.net/parquet/countries_parquet")
df.count()

# COMMAND ----------

# Append mode adds the new data to the existing data, hence why the count has doubled to 390
df.write.mode("append").format("parquet").save("abfss://data@dbstoragezat11.dfs.core.windows.net/parquet/countries_parquet")
df = spark.read.format("parquet").load("abfss://data@dbstoragezat11.dfs.core.windows.net/parquet/countries_parquet")
df.count()