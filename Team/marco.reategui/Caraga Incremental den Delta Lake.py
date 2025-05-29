# Databricks notebook source

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


df_hist = spark.read.format("delta").load("/tmp/inventario_maestro")
df_hist.orderBy("ultima_actualizacion").display()


# COMMAND ----------

#Ver versiones especificas 
df_v0 = spark.read.format("delta").option("versionAsOf",0).load("/tmp/inventario_maestro")

# COMMAND ----------

# Merge
from delta.tables import DeltaTable

# Ensure no duplicate rows for the same id_producto
df_d1_dedup = df_d1.dropDuplicates(["id_producto"])

inventario = DeltaTable.forPath(spark, "/tmp/inventario_maestro")
inventario.alias("t").merge(
    source=df_d1_dedup.alias("s"),
    condition="t.id_producto = s.id_producto"
).whenMatchedUpdate(
    condition="t.stock != s.stock or t.status != s.status",
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

# Para ver las variaciones despues de las incrementales.

df_d3 = DeltaTable.forPath(spark, "/tmp/inventario_maestro").toDF()
df_d3 = df_d3.withColumnRenamed("stock","stock_d3").withColumnRenamed("statys","status_d3")



# COMMAND ----------

df_d4 = DeltaTable.forPath(spark, "/tmp/inventario_maestro").toDF()
df_d4 = df_d3.withColumnRenamed("stock","stock_d4").withColumnRenamed("statys","status_d4")

# COMMAND ----------

variacion = df_d3.join(df_d4_estado ,on="id_producto", how="inner")\
    .select(
        "id_producto","nombre","stock_d3","status_d3","status_d4","status_d3","status_d4",
        (df_d4_estado["stock_d4"] - df_d3_estado["stock_d3"]).alias("variacion_stock"),
    )


# COMMAND ----------

df_v5 = spark.read.format("delta").option("versionAsOf",0).load("/tmp/inventario_maestro").withColumnRenamed("stock","stock_v5").withColumnRenamed("status","status_v5")

df_v6 = spark.read.format("delta").option("versionAsOf",1).load("/tmp/inventario_maestro").withColumnRenamed("stock","stock_v6").withColumnRenamed("status","status_v6")




# COMMAND ----------

#Carga con AutoLoader

from pyspark.sql.functions import input_file_name

df_stream = (spark.readStream
             .format("cloudFiles") # Activar el autoleader (Lectura incremental de la data)
             .option("cloudFiles.format", "csv") # el formato del archivo
             .option("header", "true")
             .load(" ") #es el path de origen donde se cargan archivos nuevos
             .withColumn("llegada", current_timestamp()) 
             .withColumn("fuente", input_file_name())
             
             #guardar la carga (audit trail)
             (df_stram.writeStream.format("delta")
              option("checkpointLocation", "/tmp/inventario_maestro_checkpoint/audit").start(" "))
)