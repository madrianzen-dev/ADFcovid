# Databricks notebook source
# MAGIC %md
# MAGIC **1. Carga y escritura en Delta Table con particiones**
# MAGIC
# MAGIC • Cargar el CSV y guardarlo en formato Delta particionado por categoria y ciudad.
# MAGIC
# MAGIC • Verificar que la estructura sea óptima para queries distribuidas.

# COMMAND ----------

# Cargar el archivo como dataframe
df = spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/caso_ventasCorp.csv")

# COMMAND ----------

# Guardar como Delta Table particionada por categorias.
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("categoria").save("/tmp/ventascorp")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("ciudad").save("/tmp/ventascorp")

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Actualización de precios por inflación**
# MAGIC
# MAGIC • Simular un aumento del 12% en productos de Electrónica y del 8% en Ropa.
# MAGIC
# MAGIC • Implementar con UPDATE en PySpark y comparar con SQL.

# COMMAND ----------

from delta.tables import *

ventas_delta = DeltaTable.forPath(spark, "/tmp/ventascorp")
ventas_delta.update(
    condition="categoria = 'Electrónica'",
    set={"precio_unitario": "precio_unitario * 1.12"}
)
ventas_delta.update(
    condition="categoria = 'Ropa'",
    set={"precio_unitario": "precio_unitario * 1.08"}
)


# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/tmp/ventas`
# MAGIC SET precio_unitario = precio_unitario * 1.12
# MAGIC WHERE categoria = "Electrónica"

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/tmp/ventas`
# MAGIC SET precio_unitario = precio_unitario * 1.08
# MAGIC WHERE categoria = "Ropa"

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Time Travel y Auditoría
# MAGIC
# MAGIC • Mostrar cómo eran los precios antes del ajuste.
# MAGIC
# MAGIC • Usar DESCRIBE HISTORY para explicar los cambios.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **4. Integración de nuevos datos (MERGE)**
# MAGIC
# MAGIC • Crear un lote de ventas nuevas que contenga:
# MAGIC
# MAGIC   o Una venta nueva (no existe).
# MAGIC
# MAGIC   o Una venta que actualiza datos previos (mismo venta_id).
# MAGIC
# MAGIC • Realizar MERGE INTO y explicar qué cambió.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **5. Depuración y cumplimiento legal**
# MAGIC
# MAGIC • Simular la eliminación de clientes que pidieron ser removidos (por cliente_id).
# MAGIC
# MAGIC • Aplicar DELETE y luego VACUUM con RETAIN 0 HOURS.

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **6. Optimización de rendimiento**
# MAGIC
# MAGIC • Ejecutar OPTIMIZE y ZORDER BY (cliente_id, ciudad) para mejorar búsquedas frecuentes.

# COMMAND ----------

