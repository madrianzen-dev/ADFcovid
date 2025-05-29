# Databricks notebook source
#¿Que es una UDF?
#Es una función definida por el usuario que se puede aplicar sobre un DF de spark cuando no hay una función nativa que lo resuelva.

# COMMAND ----------

#Caso 1: En un sistema de clientes, tengo algunos nombres que fueron ingresados con errores: Como "S.A" al final o en mayusculas parciales. Necesito eliminar sufijos como "S.A", "SRL" y convertirlo a un estandar

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate() #En el caso estés fuera de databricks

data = [("ZAT S.A",), ("ALICORP SRL",), ("FERRETERIA LOPEZ",),("Beta Tech",)]
df = spark.createDataFrame(data, ["razon_social"])

def normalizar_nombre(nombre):
    if not nombre:
        return None
    nombre = nombre.strip().lower()
    for sufijo in ["s.a", "srl","s.r.l"]:
        if sufijo in nombre:
            nombre = nombre.replace(sufijo, "")
    return  nombre.tittle().strip()

normalizar_udf = udf(normalizar_nombre, StringType())

df = df.withColumn("razon_social_normalizada", normalizar_udf(col("razon_social")))



# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def convertir_a_mayusculas(cadena):
    return cadena.upper()

df.withColumn("nombre_mayusculas", convertir_a_mayusculas(col("nombre"))).show(truncate=False)  
df.withColumn("nombre_mayusculas", convertir_a_mayusculas(df.["nombre"]))

# COMMAND ----------

#Transformando a @udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate() #En el caso estés fuera de databricks

data = [("ZAT S.A",), ("ALICORP SRL",), ("FERRETERIA LOPEZ",),("Beta Tech",)]
df = spark.createDataFrame(data, ["razon_social"])

@udf(returnType=StringType())
def normalizar_nombre(nombre):
    if not nombre:
        return None
    nombre = nombre.strip().lower()
    for sufijo in ["s.a", "srl","s.r.l"]:
        if sufijo in nombre:
            nombre = nombre.replace(sufijo, "")
    return  nombre.tittle().strip()

df = df.withColumn("razon_social_normalizada", normalizar_udf(col("razon_social")))


# COMMAND ----------

#Validación de RUC en Peru: Los RUC deben tener 11 digitos y validarse con un algoritmo especifico. 
@udf(returnType=StringType())
def validar_ruc_simple(ruc):
    return "Ruc bueno" if ruc and ruc.isdigit() and len(ruc) == 11 else "Error"
 
df = spark.createDataFrame([("10456789012",), ("abc",)], ["ruc"])
df = df.withColumn("Rur_Normalizado", validar_ruc_simple(col("ruc")))