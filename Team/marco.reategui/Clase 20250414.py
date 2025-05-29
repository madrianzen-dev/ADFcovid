# Databricks notebook source
from pyspark.sql.functions import current_timestamp
 
df_orders = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/orders.csv").withColumn("bronze_load_ts", current_timestamp())
df_orders.show()
 
df_orders_items = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/order items.csv").select("order_id","product_id","unit_price","quantity").withColumn("bronze_load_ts", current_timestamp())
df_orders_items.show()
#df_orders_items_transf=df_orders_items.select("order_id","product_id","unit_price","quantity").withColumn("bronze_load_ts", current_timestamp())
#df_orders_items_transf.show()
 
df_orders.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.bronze.orders_march")
df_orders_items.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.bronze.orders_items_march")

# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_items_mad where order_id>2571").select("order_id","product_id","unit_price","quantity").withColumn("silver_load_ts", current_timestamp())
df_orders_items_silver.show()
 
df_orders_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_mad where order_id>2571").select("order_id","customer_id","order_timestamp").withColumn("silver_load_ts", current_timestamp())
df_orders_silver.show()
 
df_orders_items_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_items_march")
df_orders_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_march")
 

# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT order_id as codigo_orden,product_id as codigo_producto,unit_price as precio_unitario,quantity as cantidad,unit_price*quantity as sub_total  FROM zatdev111db.silver.orders_items_march")
df_orders_items_silver.show()
 
df_orders_silver = spark.sql("SELECT order_id as codigo_maestro_orden,customer_id as codigo_cliente,order_timestamp as fecha_venta FROM zatdev111db.silver.orders_march")
df_orders_silver.show()
 
df_order_gold = df_orders_items_silver.join(df_orders_silver,df_orders_items_silver.codigo_orden==df_orders_silver.codigo_maestro_orden,"left").select("codigo_orden","codigo_producto","precio_unitario","cantidad","sub_total","codigo_cliente","fecha_venta").withColumn("gold_load_ts", current_timestamp())
df_order_gold.show()
 
df_order_gold.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.gold.consolidado_orden_march")