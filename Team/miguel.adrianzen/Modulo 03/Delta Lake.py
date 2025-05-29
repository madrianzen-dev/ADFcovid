# Databricks notebook source
#Escritura y lectura con delta tables:

#Crear un Dataframe
data = [(1,"Carlos"),(2,"Ana")]
df = spark.createDataFrame(data,["id","nombre"])

#Guardamos como Delta
df.write.format("delta").mode("overwrite").save("/tmp/usuario")
#Esto lo estás guardando dentro de un dbfs

#Leer como Delta
df_delta = spark.read.format("delta").load("/tmp/usuario")
df.delta.show()

# COMMAND ----------

#Cargar el archivo como dataframe
df = spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/ventas_complejo.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

#Guardar como Delta Table particionada por categorias.
df.write.format("delta").partitionBy("categoria").mode("overwrite").save("/tmp/ventas")

# COMMAND ----------

#Leer y mostrar algunas ventas de la categoria "Electronica"
df_electronica = spark.read.format("delta").load("/tmp/ventas").filter("categoria = 'Electrónica'")
df_electronica.display(5)


# COMMAND ----------

#Analisis de tota de ventas por ciudad:
df.groupBy("ciudad").sum("total_venta").orderBy("sum(total_venta)",asceding=False).display()

# COMMAND ----------

#Debemos hacer una actualización de precios por inflación.
#Todos los precios deben incrementarse en 10% en la categoria electronica.

from delta.tables import * 

ventas_delta = DeltaTable.forPath(spark,"/tmp/ventas")

ventas_delta.update(
    condition= "categoria='Electrónica'",
    set = {"precio_unitario" : "precio_unitario * 1.10"}	
)

# COMMAND ----------

spark.read.format("delta").load("/tmp/ventas").filter("categoria = 'Electrónica'").select("producto","precio_unitario").display(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/tmp/ventas`
# MAGIC SET precio_unitario = precio_unitario*1.10
# MAGIC where categoria='Electrónica'

# COMMAND ----------

spark.read.format("delta").load("/tmp/ventas").filter("categoria = 'Electrónica'").select("producto","precio_unitario").display(5)

# COMMAND ----------

#Llega un nuevo lote de ventas , que incluye nuevos registros y actualizas de ventas ya existentes.

nuevas_ventas  = spark.createDataFrame([
    (5,"2024-04-20",1002,"Monitor","Electrónica",2,350.00,"Lima",700.00), #Existente
    (1001,"2024-04-21",8888,"bicicleta","Deportes",1,800.00,"Cusco",800.00) #Nuevo
], ["venta_id","fecha_venta","cliente_id","producto","categoria","cantidad","precio_unitario","ciudad","total_venta"])

ventas_delta.alias("target").merge(
    nuevas_ventas.alias("source"),
    "target.venta_id = source.venta_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

#Si tenemos que hacer un Time Travel para revisión de auditoria:
#Escenario: Finanzas quiere revisar como estaban los datos antes de la actualización por inflación:

#Cargar versión anterior de la tabla
df_v_anterior = spark.read.format("delta").option("versionAsOf","0").load("/tmp/ventas")
df_v_anterior.filter("categoria = 'Electrónica'").display(5)


# COMMAND ----------

from delta.tables import DeltaTable

delta_tbl = DeltaTable.forPath(spark,"/tmp/ventas")
delta_tbl.history().display()

# COMMAND ----------

#Limpieza de datos (delete y vacuum)
#El area legal pide eliminar ventas de clientes que solicitaron ser olvidados.

ventas_delta.delete("clientes_id in (1234,5678)") #ID del cliente que necesitas

#Borrar fisicamente datos antiguos 
spark.sql("VACUUM delta.`/tmp/ventas` retain 0 hours")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Optimización para performance 
# MAGIC --El dashboard consulta muy seguido a variables como ciudad y cliente. Si quieres optimizar la lecutra:
# MAGIC
# MAGIC OPTIMIZE delta.`/tmp/ventas`
# MAGIC ZORDER BY (ciudad,cliente_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`/tmp/ventas`