# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Carga y Exploración Inicial

# COMMAND ----------

# MAGIC %md
# MAGIC **1. Carga y Exploración Inicial**
# MAGIC
# MAGIC Pregunta: Crea un DataFrame en PySpark cargando el archivo CSV desde la URL con datafctory hasta adls y leer ese archivo atravez de las rutas abfss proporcionadas. Muestra las primeras 5 filas del DataFrame y describe su esquema.
# MAGIC
# MAGIC Pista: Utiliza spark.read.csv() con las opciones adecuadas y los métodos .show() y .printSchema()

# COMMAND ----------

import os
from pyspark.sql.functions import *
from pyspark.sql import *

from pyspark.sql.types import IntegerType, FloatType

# 1. Carga y Exploración Inicial
path = "abfss://rawprz@dbstoragezat11.dfs.core.windows.net/energy/data/owid-energy-data.csv"
df = spark.read.csv(path, header=True, inferSchema=True)
df.show(5)
df.printSchema()



# COMMAND ----------

# MAGIC %md
# MAGIC **### **2. Limpieza de Datos****
# MAGIC
# MAGIC Pregunta: Identifica y maneja los valores(descartalos) nulos en el DataFrame. 
# MAGIC ¿Qué columnas contienen valores nulos y cómo los manejarías?
# MAGIC
# MAGIC Pista: Emplea los métodos .isNull(), .fillna() o .dropna().

# COMMAND ----------


valores_nulos = df.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
])
valores_nulos.show()

columnas_con_nulos = [c for c in df.columns if df.filter(col(c).isNull()).count() > 0]
print(f"Columnas con valores nulos: {columnas_con_nulos}")

df_limpio = df.dropna()

df_limpio.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df_limpio.columns
]).show()


# COMMAND ----------

# MAGIC %md
# MAGIC **### **3. Transformaciones de Datos****
# MAGIC
# MAGIC Pregunta: Crea una nueva columna llamada total_energy que sea el producto
# MAGIC de energy_supply y population.
# MAGIC
# MAGIC Pista: Utiliza withColumn() y operaciones aritméticas.

# COMMAND ----------

df_limpio.select("primary_energy_consumption", "population").printSchema()

df_transformado = (
    df_limpio
    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast("double"))
    .withColumn("population", col("population").cast("double"))
    .withColumn("total_energy", col("primary_energy_consumption") * col("population"))
)

df_transformado.select("primary_energy_consumption", "population", "total_energy").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC **### **4. Filtrado de Datos****
# MAGIC
# MAGIC Pregunta: Filtra el DataFrame para mostrar solo los países con un total_energy superior a 10,000 y una year posterior a 2000.
# MAGIC
# MAGIC Pista: Aplica el método .filter() con condiciones compuestas.

# COMMAND ----------

df_filtrado = df_transformado.filter(
    (col("total_energy") > 10000) & (col("year") > 2000)
)

df_filtrado.select("country", "year", "total_energy").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC **### **5. Agrupación y Agregación****
# MAGIC
# MAGIC Pregunta: Calcula el suministro de energía promedio por capita (energy_supply_per_capita) para cada continente.
# MAGIC
# MAGIC Pista: Emplea groupBy() y agg() con funciones de agregación como avg().

# COMMAND ----------

df_agregado = (
    df_transformado
    .groupBy("country")
    .agg(avg("energy_per_capita").alias("avg_energy_supply_per_capita"))
)

df_agregado.show()


# COMMAND ----------

# MAGIC %md
# MAGIC **### **6. Ordenamiento de Datos****
# MAGIC
# MAGIC Pregunta: Ordena los países por energy_supply_per_capita en orden descendente y muestra los 10 primeros.
# MAGIC
# MAGIC Pista: Utiliza orderBy() con el parámetro ascending=False.

# COMMAND ----------

df_top_10 = df_transformado.orderBy(col("energy_per_capita").desc())


df_top_10.select("country", "energy_per_capita").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC **### **7. Funciones de Ventana****
# MAGIC
# MAGIC Pregunta: Elimina las filas duplicadas basadas en las columnas country y year.
# MAGIC
# MAGIC Pista: Emplea el método .dropDuplicates().

# COMMAND ----------

df_sin_duplicados = df_transformado.dropDuplicates(["country", "year"])

df_sin_duplicados.select("country", "year").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC **### 8. Conversión de Tipos de Datos**
# MAGIC
# MAGIC Pregunta: Convierte la columna year a tipo IntegerType y energy_supply a FloatType.
# MAGIC
# MAGIC Pista: Utiliza cast() dentro de withColumn().

# COMMAND ----------

df_convertido = (
    df_sin_duplicados
    .withColumn("year", col("year").cast(IntegerType()))
    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast(FloatType()))
)

df_convertido.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC **### 10. Guardado de Resultados**
# MAGIC
# MAGIC Pregunta: Guarda el DataFrame resultante en formato CSV (ruta a eleccion) y crear una tabla en el catalogo

# COMMAND ----------


#  Guardado de Resultados :
output_path = "abfss://rawprz@dbstoragezat11.dfs.core.windows.net/raw/energy/data"
df.write.mode("overwrite").option("header", True).csv(output_path)

spark.sql("""
CREATE TABLE IF NOT EXISTS zatdev11db.bronze.energy_prz
USING DELTA
LOCATION 'abfss://rawprz@dbstoragezat11.dfs.core.windows.net/output/energy/data'
""")


# COMMAND ----------

spark.read.format("delta").load("abfss://rawprz@dbstoragezat11.dfs.core.windows.net/output/energy/data").display()

# COMMAND ----------

# Write some new data with mergeSchema option
data = [(1, "example")]
columns = ["id", "value"]
df = spark.createDataFrame(data, columns)

df.write.format("delta").option("mergeSchema", "true").mode("append").save("abfss://rawprz@dbstoragezat11.dfs.core.windows.net/output/energy/data")

# Now read the Delta table
df = spark.read.format("delta").load("abfss://rawprz@dbstoragezat11.dfs.core.windows.net/output/energy/data")
display(df)

# COMMAND ----------

df_convertido.display()

# COMMAND ----------

# MAGIC %md
# MAGIC