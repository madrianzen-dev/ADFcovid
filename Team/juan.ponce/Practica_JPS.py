# Databricks notebook source
# MAGIC %md
# MAGIC ## **Examen Práctico: Análisis de Datos con PySpark**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **1. Carga y Exploración Inicial**

# COMMAND ----------

# Leer archivo
file_path = "abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/energy/owid-energy-data.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

# Muestra las primeras 5 filas del DataFrame
df.show(5)

# COMMAND ----------

# describe su esquema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **2. Limpieza de Datos**

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

# ¿Qué columnas contienen valores nulos?
null_counts = df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
null_counts.show()

# COMMAND ----------

# Eliminar todas las filas que tengan al menos un valor nulo.
df_clean = df.dropna()


# COMMAND ----------

# Verificamos nuevamente los nulos después de la limpieza
df_clean.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df_clean.columns]).show()




# COMMAND ----------

# MAGIC %md
# MAGIC ### **3. Transformaciones de Datos**

# COMMAND ----------

from pyspark.sql.functions import col

# Creamos una nueva columna
df_transformed = df_clean.withColumn("total_energy",col("energy_per_capita") * col("population"))

# COMMAND ----------

# Verificamos el resultado
df_transformed.select("country","year","energy_per_capita", "population","total_energy").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **4. Filtrado de Datos**

# COMMAND ----------

df_filtrado = df_transformed.filter(
    (col("total_energy") > 10000) & (col("year") > 2000)
)

# COMMAND ----------

df_filtrado.select("country", "year", "total_energy").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **5. Agrupación y Agregación**

# COMMAND ----------

from pyspark.sql.functions import avg

df_avg_per_capita = df_transformed.groupBy("country").agg(
    avg("energy_per_capita").alias("avg_energy_per_capita")
)

# COMMAND ----------

df_avg_per_capita.show(10)

# COMMAND ----------

display(df_avg_per_capita)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **6. Ordenamiento de Datos**

# COMMAND ----------

from pyspark.sql.functions import col

df_transformed.orderBy("energy_per_capita", ascending=False).select(
    "country", "year", "energy_per_capita"
).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ### **7. Funciones de Ventana**

# COMMAND ----------

df_sin_filas_duplicados = df_transformed.dropDuplicates(["country", "year"])


# COMMAND ----------

display(df_sin_filas_duplicados)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **8. Conversión de Tipos de Datos**

# COMMAND ----------

from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import col

df_cast = df_sin_filas_duplicados.withColumn("year", col("year").cast(IntegerType())) \
                                 .withColumn("energy_cons_change_pct", col("energy_cons_change_pct").cast(FloatType()))


# COMMAND ----------

df_cast.select("year","energy_cons_change_pct").dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### **10. Guardado de Resultados**

# COMMAND ----------

df_cast.write.mode("overwrite").option("header", True).csv("/mnt/practica/practica_final_jps")



# COMMAND ----------

df_cast.write.mode("overwrite").option("header", True).csv("abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/output/practica_final_jps")


# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS zatdev11db.bronze.tabla_jps
USING CSV
OPTIONS (
  path 'abfss://rawhrc@dbstoragezat11.dfs.core.windows.net/output/practica_final_jps',
  header 'true',
  inferSchema 'true'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from zatdev11db.bronze.tabla_jps