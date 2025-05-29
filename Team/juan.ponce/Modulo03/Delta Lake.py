# Databricks notebook source
# Escritura y lectura con delta tables:
# Crear un Dataframe
date = [(1,"Carlos"),(2,"Ana")]
df = spark.createDataFrame(date,["id","nombre"])

# Guardamos como Delta
df.write.format("delta").mode("overwrite").save("/tmp/usuario")
# Esto lo estas guardando dentro de un DBFS

# Leer como Delta
df_delta = spark.read.format("delta").load("/tmp/usuario")
df_delta.show()



# COMMAND ----------

# Cargar el archivo como dataframe
df = spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/ventas_complejo.csv")



# COMMAND ----------

# Guardar como Delta Table particionada por categorias.
df.write.format("delta").mode("overwrite").partitionBy("categoria").save("/tmp/ventas")


# COMMAND ----------

# Leer y mostrar algunas ventas de la categoria "Electronica"
df_electronica = spark.read.format("delta").load("/tmp/ventas").filter("categoria = 'Electronica'")
df_electronica.display(5)



# COMMAND ----------

# Analisis de total de ventas por ciudad:
df.groupBy("ciudad").sum("total_venta").orderBy("sum(total_venta)", ascending=False).display()


# COMMAND ----------

# Debemos hacer una actualizacion de precios por inflacion.
# Todos los precios deben incrementarse en 10% en la categoria electronica.
from delta.tables import *

ventas_delta = DeltaTable.forPath(spark, "/tmp/ventas")
ventas_delta.update(
    condition="categoria = 'Electrónica'",
    set={"precio_unitario": "precio_unitario * 1.10"}
)


# COMMAND ----------

spark.read.format("delta").load("/tmp/ventas").filter("categoria = 'Electrónica'").select("producto","precio_unitario").display()



# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/tmp/ventas`
# MAGIC SET precio_unitario = precio_unitario * 1.10
# MAGIC WHERE categoria = "Electrónica"
# MAGIC

# COMMAND ----------

# Llega un nuevo lote de ventas, que incluye nuevos registros y actualiza de ventas ya existentes.
# MERGE

nuevas_ventas = spark.createDataFrame([
    (5,"2024-04-20",1002,"Monitor","Electrónica",2,350.00,"Lima",700.00),     # Existente
    (1001,"2024-04-21",8888,"bicicleta","Deportes",1,800.00,"Cusco",800.00),  # Nuevo
],['venta_id','fecha_venta','cliente_id','producto','categoria','cantidad','precio_unitario','ciudad','total_venta'])

ventas_delta.alias("target").merge

# COMMAND ----------

# Si tenemos que hacer un Time Travel para revision de auditoria: 
# Escenario: Finanzas quiere revisar como estaban los datos antes de la acutaluzacion por inflacion:

# Cargar version anterior de la tabla
df_v_anterior = spark.read.format("delta").option("versionAsOf", "0").load("/tmp/ventas")
df_v_anterior.filter("categoria = 'Electrónica'").display(5)


# COMMAND ----------

# cuantas versiones hay
from delta.tables import DeltaTable

delta_tbl = DeltaTable.forPath(spark, "/tmp/ventas")
delta_tbl.history()


# COMMAND ----------

# Limpieza de datos (delete y vacuum)
# El area legal pide eliminar ventas de clientes que solicitaron ser olvidados

ventas_Delta.delete("clientes_id in (1234,5678)") # ID del cliente
# Borrar fisicamente datos antoguos
spark.sql("VACUUM delta.`/tmp/ventas` retain 0 hours")

# COMMAND ----------

# MAGIC %sql
# MAGIC #
# MAGIC #
# MAGIC
# MAGIC OPTIMIZE delta.`/tmp/ventas` 
# MAGIC ZORDER BY (ciudad,cliente_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/tmp/ventas`
# MAGIC

# COMMAND ----------

