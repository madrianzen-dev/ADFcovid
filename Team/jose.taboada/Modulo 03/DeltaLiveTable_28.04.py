# Databricks notebook source
#Crear una base de datos
db = "deltaZAT_db_JT"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

#Preparar la base de datos para el Delta Lake
spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

# COMMAND ----------

import random

# COMMAND ----------

#Voy a importar librerias necesarias para la estructura:
import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Creamos una función donde se guardará los números aleatorios
def my_checkpoint_dir():
    return "tmp/delta_demo/chkpt_ra/%s" % str(random.randint(0, 10000))

#Creamos una función donde estarán los estados de forma aleatoria
#Declarar una función UDF de tipo string

@udf(returnType=StringType())
def random_provincias():
    return str(random.choice(["CA","TX","NY","WA"]))

#Creamos una función para empezar el query a modo de streaming , donde de forma aleatoria
#va a empezar a cargar data y lo va a asociar a tablas parquet
def genera_data_streaming(table_format,table_name,schema_ok=False,type="batch"):
    streaming_data= (spark.readStream.format("rate").option("rowsPerSecond",500).load()
                     .withColumn("loan_id",1000+col("value"))
                     .withColumn("funded_amnt",(rand()*3000+2000).cast("integer"))
                     .withColumn("paid_amnt",col("funded_amnt")-(rand()*200))
                     .withColumn("addr_state",random_provincias())
                     .withColumn("type",lit(type)))
    if schema_ok:
        streaming_data=streaming_data.select("loan_id","funded_amnt","paid_amnt","addr_state","type","timestamp")
    #Imprime el query en streaming y lo guardamos
    query = (streaming_data.writeStream
             .format(table_format)
             .option("checkpointLocation", my_checkpoint_dir())
             .trigger(processingTime="5 seconds")
             .table(table_name))
    
    return query



# COMMAND ----------

#Crear una función para gestionar el streaming

#Deter el streaming:
def stop_all_streams():
    print("Parando todo los streams - ZAT")
    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Todos los streams detenidos")
    dbutils.fs.rm("tmp/delta_demo/chkpt_ra/",True)

#funcion para limpiar las rutas y tablas totales.
def limpia_paths_tablas():
    dbutils.fs.rm("tmp/delta_demo/chkpt_ra/",True)
    #dbutils.fs.rm("file:/dbfs/tmp/delta_demo/loans_parquet/",True)

    #Voy a recorrer la tabla donde voy a limpiar los archivos
    for table in ["deltaZAT_db_RA.loans_parquet","deltaZAT_db_RA.loans_delta","deltaZAT_db_RA.loans_delta2"]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")

limpia_paths_tablas()

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet/; wget -O /dbfs/tmp/delta_demo/loans_parquet/loans.parquet https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE loans_deltara
# MAGIC using DELTA
# MAGIC as 
# MAGIC select * from parquet.`/tmp/delta_demo/loans_parquet/`

# COMMAND ----------

#Estamos cargando un archivo parquet
parquet_path = "file:/dbfs/tmp/delta_demo/loans_parquet/"
df = (spark.read.format("parquet").load(parquet_path)
      .withColumn("type",lit("batch"))
      .withColumn("timestamp",current_timestamp()))

#Lo vamos a escribir como formato delta
df.write.format("delta").mode("overwrite").saveAsTable("loans_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/tmp/delta_demo/loans_parquet/`
# MAGIC     

# COMMAND ----------

spark.sql("select count(*) from loans_delta").show()
spark.sql("select * from loans_delta").show(4)

# COMMAND ----------

#Vamos a crear escrituras en streaming de las tablas
stream_query_A = genera_data_streaming(table_format="delta",table_name="loans_delta",schema_ok=True,type='stream A')
stream_query_B = genera_data_streaming(table_format="delta",table_name="loans_delta",schema_ok=True,type='stream B')

# COMMAND ----------

display(spark.readStream.format("delta").table("loans_delta").groupBy("type").count().orderBy("type"))