# Databricks notebook source
# MAGIC %md
# MAGIC ## Selección de columnas
# MAGIC
# MAGIC El método select() de PySpark se utiliza para proyectar un conjunto de columnas desde un DataFrame, de forma similar a la cláusula SELECT de SQL.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Links and Resources
# MAGIC - [Select Function](https://spark.apache.org/docs/3.5.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html#pyspark.sql.DataFrame.select)

# COMMAND ----------

# Reading the countries_delta data into a DataFrame
df = spark.read.format("delta").load("abfss://data@dbstoragezat11.dfs.core.windows.net/delta/countries_parquet")

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using select() with String Column Names

# COMMAND ----------

campo1="country_name"
df.select(campo1, "population").display()

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using select() with Column Objects

# COMMAND ----------

from pyspark.sql.functions import col

df.select(
            col("country_name").alias("country"), 
            col("population")
         ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Attribute Access

# COMMAND ----------

df.select(df.country_name.alias("country"), df.continent).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Bracket Notation

# COMMAND ----------

df.select(df['country_name'].alias("country"), df['population']).display()