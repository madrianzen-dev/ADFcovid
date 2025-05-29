# Databricks notebook source


# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_items_mad where order_id>2571").select("order_id","product_id","unit_price","quantity").withColumn("silver_load_ts", current_timestamp())
df_orders_items_silver.show()

df_orders_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_mad where order_id>2571").select("order_id","customer_id","order_timestamp").withColumn("silver_load_ts", current_timestamp())
df_orders_silver.show()

df_orders_items_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_items_mad")
df_orders_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_mad")

# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT order_id as codigo_orden,product_id as codigo_producto,unit_price as precio_unitario,quantity as cantidad,unit_price*quantity as sub_total  FROM zatdev111db.silver.orders_items_mad")
df_orders_items_silver.show()

df_orders_silver = spark.sql("SELECT order_id as codigo_maestro_orden,customer_id as codigo_cliente,order_timestamp as fecha_venta FROM zatdev111db.silver.orders_mad")
df_orders_silver.show()

df_order_gold = df_orders_items_silver.join(df_orders_silver,df_orders_items_silver.codigo_orden==df_orders_silver.codigo_maestro_orden,"left").select("codigo_orden","codigo_producto","precio_unitario","cantidad","sub_total","codigo_cliente","fecha_venta").withColumn("gold_load_ts", current_timestamp())
df_order_gold.show()

df_order_gold.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.gold.consolidado_orden_mad")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

df_orders = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://raw@dbstoragezat11.dfs.core.windows.net/poblacion/pop_x_edad20250507-04:04.tsv").withColumn("bronze_load_ts", current_timestamp())
df_orders.show()

# COMMAND ----------

import os
from pyspark.sql import SparkSession

# Iniciar sesión Spark (omite esto si ya tienes `spark` en Databricks)
spark = SparkSession.builder.getOrCreate()

# Ruta de la carpeta
folder_path = "abfss://raw@dbstoragezat11.dfs.core.windows.net/poblacion/"

files = dbutils.fs.ls(folder_path)
#print(files)
#for f in files:
#    print(f.name, f.size, f.modificationTime)

csv_files = [f for f in files if f.path.endswith(".tsv")]
#print(csv_files)
latest_file = sorted(csv_files, key=lambda x: x.modificationTime, reverse=True)[0]
print(latest_file.name)

df_orders = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("inferSchema", "true").option("mode", "FAILFAST").load(f"abfss://raw@dbstoragezat11.dfs.core.windows.net/poblacion/"+latest_file.name).withColumn("bronze_load_ts", current_timestamp())
df_orders.display()