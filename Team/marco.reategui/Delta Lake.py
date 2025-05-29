# Databricks notebook source
# Escritura y Lectura con Detal Tables:
#Crear un DataFrame

data = [(1,"Carlos"),(2,"Maria")]
df = spark.createDataFrame(data,["id","nombre"])

#Guardar como delta
df.write.format("delta").mode("overwrite").save("/tmp/usuario_march")
#Esto se esta guardando dentro de un dbfs

#Leer como Delta
df_delta = spark.read.format("delta").load("/tmp/usuario_march")
df.delta.show()

# COMMAND ----------

#CARGAR EL ARCHIVO COMO DATAFRANE
df = spark.read.options(header="true", inferSchema="true").csv("/FileStore/tables/caso_ventasCorp.csv")

# COMMAND ----------

#GUARDAR COMO DELTA PARTICIONADA POR CATEGORIA
df.write.format("delta").partitionBy("categoria").mode("overwrite").save("/tmp/caso_ventasCorp_march")


# COMMAND ----------

#LEER Y MOSTRAR ALGUNAS VENTAS DE LA CATEGORIA "ELECTRONICA"

df_electronica = spark.read.format("delta").load("/tmp/caso_ventasCorp_march").filter("categoria='Electrónica'")
df_electronica.display(5)

# COMMAND ----------

df.groupBy("ciudad").sum("total_venta").display()

# COMMAND ----------

#DEBEMOS HACER UNA ACTUALIZACION DE PRECIOS POR INFLACION
#TODOS LOS PRECIOS DEBE INCREMENTARSE UN 10% EN LA CATEGORIA ELECTRONICA

from delta.tables import *

ventas_delta = DeltaTable.forPath(spark, "/tmp/caso_ventasCorp_march")

ventas_delta.update(
    condition = "categoria = 'Electrónica'",
    set = {"precio_unitario": "precio_unitario * 1.12"}
)
ventas_delta.update(
    condition = "categoria = 'Ropa'",
    set = {"precio_unitario": "precio_unitario * 1.08"}
)

        

# COMMAND ----------

spark.read.format("delta").load("/tmp/caso_ventasCorp_march").filter("categoria='Electrónica'").select("producto","precio_unitario").display(5)

# COMMAND ----------

#LLEGA UN NUEVO LOTE DE VENTAS, QUE INCLUYE NUEVOS REGISTROS Y ACTUALIZACIONES DE VENTAS YA EXISTENTES.

nuevas_ventas = spark.createDataFrame([
    (5,"2024-04-20",1002,"Monitor","Electrónica",1,350.0,"Lima",2520.0), #Existente
    (1001,"2024-04-21",888,"Bicicleta","Deporte",1,850.0,"Lima",3501.0), #Nuevo
],["venta_id","fecha_venta","cliente_id","producto","categoria","cantidad","precio_unitario","ciudad","total_venta"])

ventas_delta.alias("target").merge(
    
)

# COMMAND ----------

#SI TENEMOS QUE HACER UN TIME TRAVEL PARA REVISION DE AUDITORIA
#ESCENARIO: FINANZAS QUIERE REVISAR COMO ESTABAN LOS DATOS ANTES DE LA ACTUALIZACION POR INFLACION:

#CARGAR VERSION ANTERIOR DE LA TABLA
df_v_anterior = spark.read.format("delta").option("versionAsOf", "0").load("/tmp/caso_ventasCorp_march")
df_v_anterior.filter("categoria in('Electrónica','Ropa')").display(5)

# COMMAND ----------

#LIMPIEZA DE DATOS (DELETE Y VACUUM)
#EL AREA LEGAL PIDE ELIMINAR VENTAS DE CLIENTES QUE SOLICITARON SER OLVIDADOS

ventas_delta.delete("cliente_id in(28,32)") #ID del Cliente

#Borrar fisicamente datos antiguos
#spark.sql("VACUUM delta.'/tmp/caso_ventasCorp_march' RETAIN 0 HOURS")



# COMMAND ----------

# MAGIC %sql
# MAGIC --OPTIMIZACION PARA PERFORMANCE  
# MAGIC --EL DASHBOARD CONSULTA MUY SEGUIDO A VARIABLES COMO CIUDAD Y CLIENTES.
# MAGIC
# MAGIC optimize delta.