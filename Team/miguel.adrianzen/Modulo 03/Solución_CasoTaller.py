# Databricks notebook source
# Paso 1: Cargar CSV y guardar como Delta particionado por categoria y ciudad
df_csv = spark.read.option("header", True).option("inferSchema", True).csv("/mnt/data/caso_HBR_ventas.csv")

df_csv.write.format("delta").mode("overwrite").partitionBy("categoria", "ciudad").save("/tmp/ventascorp_delta")

# Validar estructura
df = spark.read.format("delta").load("/tmp/ventascorp_delta")
df.printSchema()
df.show(5)

# Paso 2: Ajuste de precios por inflación
from delta.tables import *
delta_table = DeltaTable.forPath(spark, "/tmp/ventascorp_delta")

# Electrónica +12%
delta_table.update(
    condition = "categoria = 'Electrónica'",
    set = {"precio_unitario": "precio_unitario * 1.12"}
)

# Ropa +8%
delta_table.update(
    condition = "categoria = 'Ropa'",
    set = {"precio_unitario": "precio_unitario * 1.08"}
)

# Paso 3: Time Travel - Mostrar precios antes del cambio (version 0)
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/ventascorp_delta")
df_v0.filter("categoria = 'Electrónica'").select("producto", "precio_unitario").show(5)

# Paso 4: MERGE INTO - Integrar lote nuevo
data_nueva = [(10, "2024-04-20", 1234, "Laptop", "Electrónica", 1, 1500.0, 1500.0, "Lima"),
              (1600, "2024-04-21", 8888, "Silla", "Hogar", 2, 250.0, 500.0, "Cusco")]

columns = ["venta_id", "fecha_venta", "cliente_id", "producto", "categoria", "cantidad", "precio_unitario", "total_venta", "ciudad"]
df_nueva = spark.createDataFrame(data_nueva, columns)

delta_table.alias("target").merge(
    df_nueva.alias("source"),
    "target.venta_id = source.venta_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Paso 5: Eliminar clientes por cumplimiento legal y VACUUM
clientes_borrar = [1234, 4567]
delta_table.delete(f"cliente_id in ({','.join(map(str, clientes_borrar))})")
spark.sql("VACUUM delta.`/tmp/ventascorp_delta` RETAIN 0 HOURS")

# Paso 6: OPTIMIZE y ZORDER
spark.sql("OPTIMIZE delta.`/tmp/ventascorp_delta` ZORDER BY (cliente_id, ciudad)")

# Paso 7: Reto extra - comparar ventas antes y después del cambio
from pyspark.sql.functions import sum as _sum

# Total por ciudad antes (version 0)
ventas_antiguas = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/ventascorp_delta")
ciudades_antiguas = ventas_antiguas.groupBy("ciudad").agg(_sum("total_venta").alias("total_antes"))

# Total por ciudad ahora
ventas_actuales = spark.read.format("delta").load("/tmp/ventascorp_delta")
ciudades_actuales = ventas_actuales.groupBy("ciudad").agg(_sum("total_venta").alias("total_despues"))

# Comparar
comparacion = ciudades_antiguas.join(ciudades_actuales, on="ciudad", how="inner")
comparacion = comparacion.withColumn("variacion", comparacion["total_despues"] - comparacion["total_antes"])
comparacion.orderBy("variacion", ascending=False).show(5)