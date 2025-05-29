# Databricks notebook source
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", 'P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==')

# COMMAND ----------

display(dbutils.fs.ls("abfss://rawmsb@dbstoragezat11.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC 1.- Carga y Exploracion Inicial

# COMMAND ----------

df = spark.read.option("header", True)\
               .option("inferSchema", True)\
               .option("delimiter", ",")\
               .csv("abfss://rawmsb@dbstoragezat11.dfs.core.windows.net")

# COMMAND ----------

df.printSchema()
display(df.show(5))

# COMMAND ----------

# MAGIC %md
# MAGIC 2.- Limpieza de Datos 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum
from pyspark.sql import Row

# Identificación de columnas con valores nulos
null_counts = [Row(col=c, count=df.filter(col(c).isNull()).count()) for c in df.columns]

df_nulos = spark.createDataFrame(null_counts)
display(df_nulos)

# COMMAND ----------

#Eliminar filas con valores nulos, se muestran solo 4 campos porque sino se muestra todo distorsionado.
df_sin_nulos = df.dropna()
df_sin_nulos.select("country","year","iso_code","population").show(10)

# COMMAND ----------

#reemplazando los valores nulos por un valor por defecto, se muestran solo 4 campos porque sino se muestra todo distorsionado.
df_filled = df.fillna({'iso_code': 'Unknown', 'population': 0})
df_filled.select("country","year","iso_code","population").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 3.- Transformaciones de Datos

# COMMAND ----------

from pyspark.sql.functions import col

# se utiliza el df: df_sin_nulos para poder mostrar la data de los primeros 10 registros, sino mostraria puros nulos
df_agrega_campo = df_sin_nulos.withColumn("total_energy", col("primary_energy_consumption") * col("population"))

# mostramos el nuevo campo más los otros campos para verificar que se ha creado correctamente
df_agrega_campo.select("primary_energy_consumption", "population", "total_energy").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 4.- Filtrado de Datos 
# MAGIC -Hay una dependencia con el punto 3.

# COMMAND ----------

from pyspark.sql.functions import col

df_filtrado = df.filter((col("year")>2000))
df_filtrado.show(5)



# COMMAND ----------

# MAGIC %md
# MAGIC 5.- Agrupación y Agregación 

# COMMAND ----------

from pyspark.sql.functions import avg

#Quitamos los registros nulos
df_limpios = df.filter(col("country").isNotNull() & col("energy_per_capita").isNotNull())

df_promedio = df_limpios.groupBy("country").agg(
    avg("energy_per_capita").alias("energy_supply_per_capita")
)

df_promedio.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 6.- Ordenamiento de Datos 

# COMMAND ----------

df_ordenamiento = df_promedio.orderBy("energy_supply_per_capita", ascending=False)
df_ordenamiento.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 7.- Funciones de Ventana

# COMMAND ----------

df_quita_duplicados = df.dropDuplicates(["country","year"])
df_quita_duplicados.select("country","year").show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC 8.- Conversión de Tipos de Datos 
# MAGIC --No existe un campo energy_supply

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

# Convertimos los tipos de datos
# se reemplaza el campo debido a que no existe el campo energy_supply
df_convertido = df.withColumn("year", col("year").cast(IntegerType())) \
                  .withColumn("energy_per_capita", col("energy_per_capita").cast(FloatType()))

# Mostrar el esquema para verificar el cambio de tipo de dato
df_convertido.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 10.- Guardado de Resultados 

# COMMAND ----------

# Ruta de destino en ADLS
output_path = "abfss://rawmsb@dbstoragezat11.dfs.core.windows.net/output_examen/archivo_out_examen"

# Guardar el DataFrame como CSV (con encabezado)
df_agrega_campo.write.mode("overwrite").option("header", True).csv(output_path)


# COMMAND ----------

# Guardar el DataFrame como tabla permanente
df_agrega_campo.write.mode("overwrite").saveAsTable("default.energy_archive_msb")

# COMMAND ----------

#consultamos la tabla creada
spark.sql("SELECT primary_energy_consumption, country,year,population FROM default.energy_archive_msb LIMIT 5").show()