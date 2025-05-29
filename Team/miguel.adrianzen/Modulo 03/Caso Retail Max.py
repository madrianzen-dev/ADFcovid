# Databricks notebook source
#Carguemos los archivos de un dia especifico:
from pyspark.sql.functions import current_timestamp

ruta_base= "dbfs:/FileStore/retailmax_csvs/"

#Carga todos los CSV del 2025-04-21
df_dia = (spark.read
          .option("header", "true")
          .csv(ruta_base + "*20250421.csv")
          .withColumn("llegada", current_timestamp()) )

df_dia.show()       

# COMMAND ----------

#Creamos la tabla maestra inicial delta
df_dia.write.format("delta").mode("overwrite").save("/tmp/CasoRetailMax/stock_consolidado")


# COMMAND ----------

spark.read.format("delta").load("/tmp/CasoRetailMax/stock_consolidado").display()

# COMMAND ----------

#Simular la carga incremental (del dia siguiente)
df_incremental = (spark.read
        .option("header","true")
        .csv(ruta_base+"*20250422.csv")
        .withColumn("llegada", current_timestamp()))
        

# COMMAND ----------

# Aplicar el merge:
from delta.tables import DeltaTable

# Deduplicate the source DataFrame
df_incremental_dedup = df_incremental.dropDuplicates(["id_producto"])

delta_maestra = DeltaTable.forPath(spark, "/tmp/CasoRetailMax/stock_consolidado")

delta_maestra.alias("t").merge(
    df_incremental_dedup.alias("s"),
    "t.id_producto = s.id_producto"
).whenMatchedUpdate(
    condition="t.stock != s.stock OR t.estado != s.estado",
    set={
        "nombre": "s.nombre",
        "stock": "s.stock",
        "estado": "s.estado",
        "llegada": "s.llegada"
    }
).whenNotMatchedInsert(
    values={
        "id_producto": "s.id_producto",
        "nombre": "s.nombre",
        "stock": "s.stock",
        "estado": "s.estado",
        "llegada": "s.llegada"
    }
).execute()

display(delta_maestra.toDF())

# COMMAND ----------

#Creamos una tabla audit Trail

df_audit = (spark.read
        .option("header","true")
        .csv(ruta_base+"*20250421.csv")
        .withColumn("llegada", current_timestamp()))

df_audit.write.format("delta").mode("append").save("/tmp/CasoRetailMax/stock_audit")         

# COMMAND ----------

#Diferencias entre canales:
spark.sql("""
SELECT id_producto,nombre, count(DISTINCT stock) as versiones_stock
from delta.`/tmp/CasoRetailMax/stock_audit`         
group by id_producto, nombre
HAVING versiones_stock > 1          
""").display()

# COMMAND ----------

#conflicto de estado entre canales:
spark.sql("""
SELECT id_producto,count(DISTINCT estado) as estados
from delta.`/tmp/CasoRetailMax/stock_audit`
group by id_producto
HAVING estados > 1
""").display()

# COMMAND ----------

#canal con menor stock
spark.sql("""
SELECT canal, avg(stock) as promedio_stock
from delta.`/tmp/CasoRetailMax/stock_audit`
group by canal
order by promedio_stock
""").display()

# COMMAND ----------

#Rollback de version:
spark.read.format("delta").option("versionAsOf",0).load("/tmp/CasoRetailMax/stock_consolidado").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Beneficios de stock_audit:
# MAGIC 1. Permite trazabilidad
# MAGIC 2. Base para reconciliación de errores
# MAGIC 3. Soporte a auditorias regulatorias
# MAGIC

# COMMAND ----------

#Nombre distinto con mismo ID
spark.sql("""
SELECT id_producto,count(DISTINCT nombre) as versiones_nombre
from delta.`/tmp/CasoRetailMax/stock_audit`
group by id_producto
HAVING versiones_nombre > 1
""").display()