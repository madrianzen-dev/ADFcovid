# Databricks notebook source
# MAGIC %md
# MAGIC # Examen Práctico: Análisis de Datos con PySpark 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Conjunto de Datos**
# MAGIC Utiliza el archivo CSV desde GitHub:
# MAGIC https://raw.githubusercontent.com/owid/energy-data/refs/heads/master/owid-energy-data.csv
# MAGIC
# MAGIC ###**Requisitos**
# MAGIC * Tener acceso a un entorno de ejecución, como Databricks
# MAGIC * Tener acceso a un entorno de ejecución, como Datafactory
# MAGIC
# MAGIC ### PIPELINE DE INGESTA DESDE GIT: "**Ingesta_Git_Energy_FG**"
# MAGIC ### STORAGE: "https://dbstoragezat11.blob.core.windows.net/data/FG_TEST/"

# COMMAND ----------

# MAGIC %md
# MAGIC **Librerias**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import count, when, isnan
from pyspark.sql import Window
from pyspark.sql.functions import row_number
import pandas as pd


# COMMAND ----------

# MAGIC %md
# MAGIC ##  1. Carga y Exploración Inicial 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Crea un DataFrame en PySpark cargando el archivo CSV desde la URL con datafctory hasta
# MAGIC adls y leer ese archivo atravez de las rutas abfss proporcionadas. Muestra las primeras 5 filas del
# MAGIC DataFrame y describe su esquema.
# MAGIC
# MAGIC _Pista:_ Utiliza spark.read.csv() con las opciones adecuadas y los métodos .show() y .printSchema()

# COMMAND ----------


# url = "https://raw.githubusercontent.com/owid/energy-data/master/owid-energy-data.csv"
#df = spark.createDataFrame(pd.read_csv(url))

path_adls = "abfss://data@dbstoragezat11.dfs.core.windows.net/FG_TEST/csv_in/energy_data_FG.csv"
df = spark.read.option("header", True).option("inferSchema", True).csv(path_adls)

df.show(5)
df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpieza de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Identifica y maneja los valores(descartalos) nulos en el DataFrame. ¿Qué columnas
# MAGIC contienen valores nulos y cómo los manejarías?
# MAGIC
# MAGIC _Pista:_ Emplea los métodos .isNull(), .fillna() o .dropna()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Obtener nombres de columnas que tienen al menos un valor nulo
columns_with_nulls = [column for column in df.columns if df.where(col(column).isNull()).count() > 0]

print("Columnas con valores nulos:", columns_with_nulls)

# Eliminar filas con valores nulos
df_clean = df.na.drop()

# COMMAND ----------

display(df_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformaciones de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Crea una nueva columna llamada total_energy que sea el producto
# MAGIC de energy_supply y population.
# MAGIC
# MAGIC _Pista:_ Utiliza withColumn() y operaciones aritméticas.

# COMMAND ----------

# MAGIC %md
# MAGIC **DADO QUE NO EXISTE "energy_supply" SE PROCEDERÁ A USAR "energy_per_capita", QUE PARECE LO MÁS CERCANO:**

# COMMAND ----------



df_transformed = df_clean.withColumn("total_energy", col("energy_per_capita") * col("population"))

# COMMAND ----------

display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Filtrado de Datos

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Filtra el DataFrame para mostrar solo los países con un total_energy superior a 10,000 y
# MAGIC una year posterior a 2000.
# MAGIC
# MAGIC _Pista:_ Aplica el método .filter() con condiciones compuestas.

# COMMAND ----------

# total_energy > 10000 y año > 2000
df_filtered = df_transformed.filter((col("total_energy") > 10000) & (col("year") > 2000))

# COMMAND ----------

display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Agrupación y Agregación 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Calcula el suministro de energía promedio por capita (energy_supply_per_capita) para cada
# MAGIC continente.
# MAGIC
# MAGIC _Pista:_ Emplea groupBy() y agg() con funciones de agregación como avg().

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por pais

# COMMAND ----------

df_grouped_country = df_filtered.groupBy("country").agg(avg("energy_per_capita").alias("avg_energy_per_capita"))


# COMMAND ----------

display(df_grouped_country)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Por continente:

# COMMAND ----------

df_grouped_conti = df_clean.withColumn(
    "continent",
    when(col("iso_code").startswith("A"), "Asia")
    .when(col("iso_code").startswith("E"), "Europe")
    .when(col("iso_code").startswith("F"), "Africa") 
    .when(col("iso_code").startswith("N"), "North America")
    .when(col("iso_code").startswith("S"), "South America")
    .otherwise("Other")
)

energy_x_conti = (df_grouped_conti
    .groupBy("continent")
    .agg(avg(col("energy_per_capita")).alias("average_energy_per_capita"))
    .orderBy("average_energy_per_capita", ascending=False)
)

display(energy_x_conti)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ordenamiento de Datos 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Ordena los países por energy_supply_per_capita en orden descendente y muestra los 10
# MAGIC primeros.
# MAGIC
# MAGIC _Pista:_ Utiliza orderBy() con el parámetro ascending=False

# COMMAND ----------

df_with_energy_per_capita = df_filtered.withColumn(
    "energy_supply_per_capita", 
    col("primary_energy_consumption") / col("population")
)

# Ordenamos y seleccionamos los top 10
top_10 = (df_with_energy_per_capita
    .orderBy(col("energy_supply_per_capita").desc())
    .limit(10)
)

display(top_10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Funciones de Ventana 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Elimina las filas duplicadas basadas en las columnas country y year.
# MAGIC
# MAGIC _Pista:_ Emplea el método .dropDuplicates().

# COMMAND ----------

df_no_duplicates = top_10.dropDuplicates(["country", "year"])
display(df_no_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conversión de Tipos de Datos 

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Convierte la columna year a tipo IntegerType y energy_supply a FloatType.
# MAGIC
# MAGIC _Pista:_ Utiliza cast() dentro de withColumn().

# COMMAND ----------

# MAGIC %md
# MAGIC **SE ESTA USANDO primary_energy_consumption EN LUGAR DE "energy_supply"**

# COMMAND ----------


df_casted = df_no_duplicates.withColumn("year", col("year").cast(IntegerType())) \
                    .withColumn("primary_energy_consumption", col("primary_energy_consumption").cast(FloatType()))

display(df_casted)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Guardado de Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** Guarda el DataFrame resultante en formato CSV (ruta a eleccion) y crear una tabla en el
# MAGIC catalogo

# COMMAND ----------

output_path = "abfss://data@dbstoragezat11.dfs.core.windows.net/FG_TEST/csv_out/energy_data"

df_casted.write.mode("overwrite").option("header", True).csv(output_path)
