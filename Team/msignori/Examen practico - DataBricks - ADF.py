# Databricks notebook source
#1
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", 'P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==')

# COMMAND ----------

#1
display(dbutils.fs.ls("abfss://msl-contenedor@dbstoragezat11.dfs.core.windows.net"))

# COMMAND ----------

#1
df = spark.read.option("header", True)\
               .option("inferSchema", True)\
               .option("delimiter", ",")\
               .csv("abfss://msl-contenedor@dbstoragezat11.dfs.core.windows.net")



# COMMAND ----------

#1
# Mostrar las primeras 5 filas del DataFrame (Se deja como comentario porque al mostrar todo el plano no se visualiza correctamente)
#df.show(5)

# Se muestran 3 columnas para mostrar un dataframe ordenado
df.select("country", "year", "iso_code").show(5)

# COMMAND ----------

#1
df.printSchema()

# COMMAND ----------

#2

from pyspark.sql.functions import col, sum as _sum
from pyspark.sql import Row

# Contar valores nulos por columna y convertir a lista de Row
null_counts = [
    Row(column=c, null_count=df.filter(col(c).isNull()).count())
    for c in df.columns
]

# Crear un nuevo DataFrame con los resultados
nulls_df = spark.createDataFrame(null_counts)

# Mostrar las columnas con valores nulos
nulls_df.filter(col("null_count") > 0).show()

# COMMAND ----------

#2
#Borrar registros nulos
df = df.na.drop()

#
#df.show(5)
df.select("country", "year", "iso_code").show(5)

# COMMAND ----------

#3
from pyspark.sql.functions import col

# Crear una nueva columna 'total_energy' = primary_energy_consumption * population
df = df.withColumn("total_energy", col("primary_energy_consumption") * col("population"))

# Mostrar algunas filas para verificar
df.select("country", "year", "primary_energy_consumption", "population", "total_energy").show(5)


# COMMAND ----------

#4
from pyspark.sql.functions import col

# Filtrar países con total_energy > 10000 y year > 2000
df_filtered = df.filter((col("total_energy") > 10000) & (col("year") > 2000))

# Mostrar resultados
df_filtered.select("country", "year", "total_energy").show(10)

# COMMAND ----------

#5

from pyspark.sql.functions import col, avg

# Agrupar por país y calcular el promedio de energy_per_capita
result = df.groupBy("country") \
           .agg(
             avg(col("energy_per_capita")).alias("prom_energy_per_capita")
           )

# Mostrar el resultado
result.show()

# COMMAND ----------

#6

df.orderBy("energy_per_capita", ascending=False) \
  .select("country", "energy_per_capita") \
  .show(10)

# COMMAND ----------

#7

# Eliminar filas duplicadas basadas en country, year
df_sin_duplicados = df.dropDuplicates(["country", "year"])

# COMMAND ----------

#8
from pyspark.sql.types import IntegerType, FloatType

# Convertir year a Int y primary_energy_consumption(energy_supply) a Float
df = df.withColumn("year", df["year"].cast(IntegerType())) \
       .withColumn("primary_energy_consumption", df["primary_energy_consumption"].cast(FloatType()))


# COMMAND ----------

#10

# Ruta de salida (puedes cambiarla a tu propia ruta abfss o local)
output_path = "abfss://msl-contenedor@dbstoragezat11.dfs.core.windows.net/final/energy_data_clean.csv"

# Guardar como CSV (sin cabeceras duplicadas, en una sola carpeta si es necesario)
df.write.mode("overwrite").option("header", True).csv(output_path)

# COMMAND ----------

#10
# Guardar el DataFrame como tabla permanente
df.write.mode("overwrite").saveAsTable("default.energy_data_temp_msl")

# COMMAND ----------

#10
spark.sql("SHOW TABLES IN default").show()

# COMMAND ----------

#10
spark.sql("SELECT country,year,population FROM default.energy_data_temp_msl LIMIT 5").show()