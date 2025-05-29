# Databricks notebook source
# MAGIC %md
# MAGIC UDF : 
# MAGIC
# MAGIC Funcion Definida por el Usuario, que se puede aplicar sobre un DF de Spark cuando 

# COMMAND ----------

# Cas0 1: En un sistema de clientes, tengo algunos nombres que fueron ingresados con errores: Como "S.A" al final o en mayusculas parciales. Necesito eliminar sufijos como "S.A", "SRL" y convertirlo a un estandar


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate() # En el caso ester fuera de databricks

data = [("ZAT S.A",),("ALICORP SRL",),("FERRETERIA LOPEZ",),("Veta Tech",)]
df = spark.createDataFrame(data, ["razon_social"])

def normalizar_nombre(nombre):
    if not nombre:
        return None
    nombre = nombre.strip().lower()
    for sufijo in ["s.a", "srl","s.r.l"]:
        if sufijo in nombre:
            nombre = nombre.replace(sufijo, "")
    return nombre.tittle().strip()

normalizar_udf = udf(normalizar_nombre, StringType())

df = df.withColumn("razon_social", normalizar_udf(col("razon_social")))



# COMMAND ----------

df.show()


# COMMAND ----------

# cambiar con funcion UDF
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

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def convertir_a_mayusculas(cadena):
    return cadena.upper()

df.withColumn("nombre_mayusculas", convertir_a_mayusculas(col("nombre"))).show(truncate=false)
df.withColumn("nombre_mayusculas", convertir_a_mayusculas(df("nombre")))


# COMMAND ----------

# Validacion de RUC en Peru: Los RUC deben tener 11 digitos y validarse con un algoritmo especifico.

@udf(returnType=StringType())
def validar_ruc(ruc):
    if not ruc:
        return False
    if len(ruc) != 11:
        return False
    return True

df = spark.createDataFrame([("10456789012",), ("abc",)], ["ruc"])
df = df.withColumn("ruc_valido", validar_ruc(col("ruc")))


# COMMAND ----------

@udf(returnType=StringType())
def validar_ruc_simple(ruc):
    return "Ruc bueno" if ruc and ruc.isdigit() and len(ruc) == 11 else "Error"
 
df = spark.createDataFrame([("10456789012",), ("abc",)], ["ruc"])
df = df.withColumn("Rur_Normalizado", validar_ruc_simple(col("ruc")))
 