# Databricks notebook source
# MAGIC %md
# MAGIC ### **1. Carga y Exploracion Inicial**

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

ruta = "abfss://rawmarch@dbstoragezat11.dfs.core.windows.net/Data_Energy/energy_data.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(ruta)
df.show(5)
df.printSchema()



# COMMAND ----------

# MAGIC %md
# MAGIC ## **2. Limpieza de Datos**

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Contar valores nulos por columna
columnas_nulos = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
columnas_nulos.show()

df_valida = df.dropna()

df_valida.select([ sum(col(c).isNull().cast("int")).alias(c) for c in df_valida.columns]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Transformacion de Datos

# COMMAND ----------

#Creando una nueva columna 
df_valida.select("primary_energy_consumption", "population").printSchema()

df_real = (
    df_valida
    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast("double"))
    .withColumn("population", col("population").cast("double"))
    .withColumn("total_energy", col("primary_energy_consumption") * col("population"))
)

df_real.select("primary_energy_consumption", "population", "total_energy").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Filtrado de Datos 

# COMMAND ----------

# Filtrar informacion
df_data_filtrada = df_real.filter((col("total_energy") > 1000) & (col("year") > 2000))

df_data_filtrada.select("total_energy", "year").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Agrupacion y Agregación

# COMMAND ----------

#Informacion Agrupados
df_data_agregada = (
    df_real
    .groupBy("country")
    .agg(avg("energy_per_capita").alias("promedio_energy_supply_per_capita"))
)

df_data_agregada.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Ordenamiento de Datos.

# COMMAND ----------

#ordenar informacion 
df_data_ordenada = df_real.orderBy(col("energy_per_capita").desc())
df_data_ordenada.select("country", "energy_per_capita").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Funciones de Ventana

# COMMAND ----------

# Filtrar informacion sin Duplicar
df_data_sin_duplicar = df_real.dropDuplicates(["country", "year"])
df_data_sin_duplicar.select("country", "year").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Conversion de Tipos de Datos

# COMMAND ----------

#Convertir tipos de datos.
df_convertido = (
    df_data_sin_duplicar
    .withColumn("year", col("year").cast(IntegerType()))
    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast(FloatType()))
)

df_convertido.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Guardado de Resultado
# MAGIC

# COMMAND ----------

#Guardar la informacion en la tabla final 
output_path = "abfss://rawmarch@dbstoragezat11.dfs.core.windows.net/raw/energy/data_energy"
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.sql("""
CREATE TABLE IF NOT EXISTS zatdev11db.bronze.data_energy_march
USING DELTA
LOCATION 'abfss://rawmarch@dbstoragezat11.dfs.core.windows.net/output/energy/data_energy'
""")
