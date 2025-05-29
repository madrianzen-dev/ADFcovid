# Databricks notebook source
#Crear una Base de datos

db = "deltaZAT_1_db"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

#Preparar a Base de Datos para el uso de Delta Lake
spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spart.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")



# COMMAND ----------

#Voy a importar librerias necesarias para la estructura:
import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Creamos una funcion donde se  guardara lis numeros aleatorios
def my_checkpoint_dir()
return "tmp/delta_demo/chkpt/%s" % str(random.randint(0, 1000))

#creamos una funcion donde estaran los estados de forma aleatoria
#declarar una funcion UDF de tipo String.

@udf(returnType=StringType())
def random_provincias():
    return str(random.choice(["CA","TX","NY","WA"]))

#creamos una funcion para emprsas el query a modo de streaming, donde de forma aleatoria
#va a emprezar a cargar data y lo va a asociar a tabla parquet.

def genera_data_streaming(table_format,table_name,schema_ok=False,type="batch"):
    streaming_data=(spark.readStream.format("rate").option("rowsPerSecond",500).load()
        .withColumn("load_id",1000+col("value"))
        .withColumn("funded_amnt",(rand()*3000+2000).cast("integer"))
        .withColumn("pain_amnt",col("funded_amnt")-(rand()*200))
        .withColumn("addr_state",random_provincias())
        .withColumn("dt",lit(datetime.now()))
        .withcolumn("type",lit(type))
        )
if schema_ok:
    streaming_data=streaming_data.select("load_id","funded_amnt","pain_amnt","addr_state","type","timestamp")
    #Imprime el query en streaming y lo guardamos
    query = (streaming_data.writeStream
        .format(table_format)
        .option("checkpointLocation",my_checkpoint_dir())
        .trigger(processingTime="5 seconds")
        .table(table_name))
    return query      


# COMMAND ----------

#crear una funcion para gestionar el streaming

#Detener el streaming
def stop_all_streams():
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass    
    print("Todos los Streams detenidos")
    dbutils.fs.rm("tmp/delta_demo/chkpt",True)

#Funcion para limpiar las rutas y tablas totales
def limpiar_paths_tablas():
    dbutils.fs.rm("tmp/delta_demo/chkpt",True)
    dbutils.fs.rm("file:/dbfs/delta_demo/loans_parquet/",True)

    #Voy a recorrer la tabla donde voy a limpiar los archivos
    for table in["deltaZAT_1_db.loans_parquet","deltaZAT_1_db.loans_delta","deltaZAT_1_db.loans_delta2"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

limpiar_paths_tablas

