# Databricks notebook source
# MAGIC %md
# MAGIC Caso 1: Sistema de Inventario multisucursal:
# MAGIC Tienes archivos por día provenientes de distintas sucursales con stock de productos.
# MAGIC Se busca mantener una tabla maestra consolidada en delta:
# MAGIC 1. Si llega un producto con stock distinto al actual, se actualiza el valor y se registra en un timestamp.
# MAGIC 2. Si es nuevo, se inserta
# MAGIC 3. Si llega un producto con stock igual, se ignora
# MAGIC 4. Los productos dados de baja (status="removido") deben ser marcados como inactivos.
# MAGIC

# COMMAND ----------

#Simulación de una tabla maestra:
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()

data_maestra = [
    (1001,"Laptop Gamer",20,"ACTIVO"),
    (1002,"Mouse inalambrico",50,"ACTIVO"),
    (1003,"Teclado mecanico",35,"ACTIVO"),
]

cols = ["id_producto","nombre","stock","status"]
df_maestra = spark.createDataFrame(data_maestra,cols).withColumn("ultima_actualizacion",current_timestamp())
df_maestra.write.format("delta").mode("overwrite").save("/tmp/inventario_maestro")

# COMMAND ----------

#Simulación de carga incremental (dia 1)
from pyspark.sql.functions import lit

data_incrementa_d1=[
    (1001,"Laptop Gamer",18,"ACTIVO"),      #stock cambio
    (1002,"Mouse inalambrico",50,"ACTIVO"), #sin cambios
    (1003,"Monitor 27",12,"ACTIVO"),        #nuevo
    (1003,"Teclado mecanico",35,"REMOVIDO") #Marcado para baja   
]

df_d1 = spark.createDataFrame(data_incrementa_d1,cols)
df_d1 = df_d1.withColumn("llegada",current_timestamp())


# COMMAND ----------

df_d1.select("id_producto","nombre","stock","status","llegada").display()

# COMMAND ----------

df_hist = spark.read.format("delta").load("/tmp/inventario_maestro")
df_hist.orderBy("ultima_actualizacion").display()

# COMMAND ----------

#Ver versiones especificas 
df_v0 = spark.read.format("delta").option("versionAsOf",0).load("/tmp/inventario_maestro").display()

# COMMAND ----------

# Merge
from delta.tables import DeltaTable

# Deduplicate the source DataFrame
df_d1_dedup = df_d1.dropDuplicates(["id_producto"])

inventario = DeltaTable.forPath(spark, "tmp/inventario_maestro")

inventario.alias("t").merge(
    source=df_d1_dedup.alias("s"),
    condition="t.id_producto = s.id_producto"
).whenMatchedUpdate(
    condition="t.stock != s.stock OR t.status != s.status",
    set={
        "nombre": "s.nombre",
        "stock": "s.stock",
        "status": "s.status",
        "ultima_actualizacion": "s.llegada"
    }
).whenNotMatchedInsert(
    values={
        "id_producto": "s.id_producto",
        "nombre": "s.nombre",
        "stock": "s.stock",
        "status": "s.status",
        "ultima_actualizacion": "s.llegada"
    }
).execute()

display(inventario.toDF())

# COMMAND ----------

#Para ver variaciones luego de la gestión incremental:
df_d3 = DeltaTable.forPath(spark, "tmp/inventario_maestro").toDF()
df_d3 = df_d3.withColumnRenamed("stock","stock_d3").withColumnRenamed("status","status_d3")

# COMMAND ----------

# Merge para caso variacion
from delta.tables import DeltaTable

# Deduplicate the source DataFrame
df_d1_dedup = df_d1.dropDuplicates(["id_producto"])

inventario = DeltaTable.forPath(spark, "tmp/inventario_maestro")

inventario.alias("t").merge(
    source=df_d1_dedup.alias("s"),
    condition="t.id_producto = s.id_producto"
).whenMatchedUpdate(
    condition="t.stock != s.stock OR t.status != s.status",
    set={
        "nombre": "s.nombre",
        "stock": "s.stock",
        "status": "s.status",
        "ultima_actualizacion": "s.llegada"
    }
).whenNotMatchedInsert(
    values={
        "id_producto": "s.id_producto",
        "nombre": "s.nombre",
        "stock": "s.stock",
        "status": "s.status",
        "ultima_actualizacion": "s.llegada"
    }
).execute()

display(inventario.toDF())

# COMMAND ----------

df_d4_estado = DeltaTable.forPath(spark, "tmp/inventario_maestro").toDF()
df_d4_estado = df_d4_estado.withColumnRenamed("status","status_d4").withColumnRenamed("stock","stock_d4")

# COMMAND ----------

variacion = df_d3.join(df_d4_estado,on = "id_producto",how = "inner") \
    .select(
        "id_producto",df_d4_estado["nombre"].alias("nombre"),"stock_d3","stock_d4",
        "status_d3","status_d4",
        (df_d4_estado["stock_d4"] - df_d3["stock_d3"]).alias("variacion_stock")
    )

variacion.show()

# COMMAND ----------

df_v5 = spark.read.format("delta").option("versionAsOf",0).load("/tmp/inventario_maestro").withColumnRenamed("stock","stock_v5")

df_v6 = spark.read.format("delta").option("versionAsOf",1).load("/tmp/inventario_maestro").withColumnRenamed("stock","stock_v6")

variacion = df_v5.join(df_v6,on = "id_producto",how = "outer") \
    .select(

        "id_producto",df_v6["nombre"].alias("nombre"),"stock_v5","stock_v6",
         (df_v6["stock_v6"] - df_v5["stock_v5"]).alias("variacion_stock")
    )

variacion.show()

# COMMAND ----------

#Carga con AutoLoader  
from pyspark.sql.functions import input_file_name

df_stream = (spark.readStream
    .format("cloudFiles") # Activar el Auto Loader (lectura incremental de la data)
    .option("cloudFiles.format", "csv") #el formato del archivo
    .option("header","true")
    .load(" ") #es el path de origen donde se cargan archivos nuevos 
    .withColumn("llegada",current_timestamp())
    .withColumn("fuente",input_file_name()))

#Guardar la carga (audit trail)
(df_stream.writeStream.format("delta")
    .option("checkpointLocation", "/tmp/inventario_maestro_checkpoint/audit").start(" "))


# COMMAND ----------

# MAGIC %sql
# MAGIC --Si quiero ejecutar una carga puntual con COPY INTO desde un storage externo:
# MAGIC --Tabla destino ya creada en delta:
# MAGIC
# MAGIC create or replace table inventario_ext (
# MAGIC   id int, nombre string, stock int, estado string
# MAGIC ) using DELTA location 'mnt/data/inventario_ext'
# MAGIC
# MAGIC ---Simulación de ingesta:
# MAGIC COPY INTO inventario_ext
# MAGIC FROM '/mnt/data/inventario_ext'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header'='true')
# MAGIC COPY_OPTIONS ('mergeSchema'='true')
# MAGIC

# COMMAND ----------

#Simulación de carga incremental (dia 1)
from pyspark.sql.functions import lit