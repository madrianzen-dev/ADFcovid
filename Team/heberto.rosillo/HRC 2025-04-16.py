# Databricks notebook source
from pyspark.sql.functions import current_timestamp
 
df_orders = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/orders.csv").withColumn("bronze_load_ts", current_timestamp())
df_orders.show()
 
df_orders_items = spark.read.format("csv").option("header", "true").option("delimiter", ";").option("inferSchema", "true").option("mode", "FAILFAST").load("abfss://inputs@dbstoragezat11.dfs.core.windows.net/datamed/order items.csv").select("order_id","product_id","unit_price","quantity").withColumn("bronze_load_ts", current_timestamp())
df_orders_items.show()
#df_orders_items_transf=df_orders_items.select("order_id","product_id","unit_price","quantity").withColumn("bronze_load_ts", current_timestamp())
#df_orders_items_transf.show()
 
df_orders.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.bronze.orders_hrc")
df_orders_items.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.bronze.orders_items_hrc")

# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_items_mad where order_id>2571").select("order_id","product_id","unit_price","quantity").withColumn("silver_load_ts", current_timestamp())
df_orders_items_silver.show()
 
df_orders_silver = spark.sql("SELECT * FROM zatdev111db.bronze.orders_mad where order_id>2571").select("order_id","customer_id","order_timestamp").withColumn("silver_load_ts", current_timestamp())
df_orders_silver.show()
 
df_orders_items_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_items_hrc")
df_orders_silver.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.silver.orders_hrc")

# COMMAND ----------

df_orders_items_silver = spark.sql("SELECT order_id as codigo_orden,product_id as codigo_producto,unit_price as precio_unitario,quantity as cantidad,unit_price*quantity as sub_total  FROM zatdev111db.silver.orders_items_hrc")
df_orders_items_silver.show()
 
df_orders_silver = spark.sql("SELECT order_id as codigo_maestro_orden,customer_id as codigo_cliente,order_timestamp as fecha_venta FROM zatdev111db.silver.orders_hrc")
df_orders_silver.show()
 
df_order_gold = df_orders_items_silver.join(df_orders_silver,df_orders_items_silver.codigo_orden==df_orders_silver.codigo_maestro_orden,"left").select("codigo_orden","codigo_producto","precio_unitario","cantidad","sub_total","codigo_cliente","fecha_venta").withColumn("gold_load_ts", current_timestamp())
df_order_gold.show()
 
df_order_gold.write.mode("overwrite").format("delta").saveAsTable("zatdev111db.gold.consolidado_orden_hrc")

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import year
from pyspark.sql.functions import month, count
df = df.withColumn("Fecha_Registro", to_date(col("Fecha_Registro"), "yyyy-MM-dd"))
df_2023 = df.filter(year("Fecha_Registro") == 2023)
registros_por_mes = df_2023.groupBy(month("Fecha_Registro").alias("Mes")).agg(count("*").alias("Total_Registros"))


# COMMAND ----------

df.createOrReplaceTempView("clientes")

df_cliente_perfilado = spark.sql("""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY `País` ORDER BY Edad DESC) AS rn
        FROM clientes
    ) sub
    WHERE rn <= 3
""")

df_cliente_perfilado.show()

# COMMAND ----------

spark.sql("""
    SELECT 
        regexp_extract(Correo, '@(.+)', 1) AS Dominio,
        COUNT(*) AS Total
    FROM clientes
    GROUP BY Dominio
    ORDER BY Total DESC
""").show()

# COMMAND ----------

spark.sql("""
    SELECT 
        CASE 
            WHEN Fecha_Registro < '2024-01-01' THEN 'Menor al 2024'
            ELSE 'Mayor a  2024'
        END AS Periodo,
        COUNT(*) AS Total
    FROM clientes
    GROUP BY Periodo
""").show()

# COMMAND ----------

spark.sql("""
    WITH Ranking AS (
        SELECT 
            `País`,
            Edad,
            ROW_NUMBER() OVER (PARTITION BY `País` ORDER BY Edad DESC) / COUNT(*) OVER (PARTITION BY `País`) AS RankingValor
        FROM clientes
    )
    SELECT 
        `País`,
        CASE 
            WHEN RankingValor > 0.8 THEN 'Alto'
            WHEN RankingValor > 0.5 THEN 'Medio'
            ELSE 'Bajo'
        END AS Clasificacion,
        COUNT(*) AS TotalClientes
    FROM Ranking
    GROUP BY `País`, Clasificacion
    ORDER BY `País`, Clasificacion
""").show()

# COMMAND ----------

spark.sql("""
    SELECT 
        Correo,
        CASE 
            WHEN Correo IS NULL THEN 'Null'
            WHEN NOT Correo LIKE '%@%.%' THEN 'Error'
            WHEN Correo LIKE '%tempmail%' OR Correo LIKE '%fake%' THEN 'Sospechoso'
            ELSE 'Bueno'
        END AS EstadoCorreo
    FROM clientes
""").show(truncate=False)

# COMMAND ----------


genero_por_pais = spark.sql("""
    SELECT 
         `País`,
        `Género`,
        COUNT(*) AS Total
    FROM clientes
    GROUP BY  `País`,`Género`
""")

genero_pd = genero_por_pais.toPandas()
genero_pd['Proporcion'] = genero_pd.groupby('País')['Total'].transform(lambda x: x / x.sum())

genero_pd.sort_values(by=['País', 'Proporcion'], ascending=[True, False]).head(10)

# COMMAND ----------

from pyspark.sql import functions as F
proporciones_df = spark.sql("""
WITH totales AS (
    SELECT 
        `País`,
        `Género`,
        COUNT(*) AS Total
    FROM clientes
    GROUP BY `País`, `Género`
),
proporciones AS (
SELECT 
    `País`,
    `Género`,
    Total,
    Total * 1.0 / SUM(Total) OVER (PARTITION BY `País`) AS Proporcion
FROM totales
) SELECT * FROM proporciones
""")

pivot_df = proporciones_df.groupBy("País").pivot("Género").agg(F.first("Proporcion"))
pivot_df.show()


# COMMAND ----------

from scipy.stats import entropy
import pandas as pd

genero_por_pais = spark.sql("""
    SELECT 
         `País`,
        `Género`,
        COUNT(*) AS Total
    FROM clientes
    GROUP BY  `País`,`Género`
""")

genero_pd = genero_por_pais.toPandas()

genero_pd['Proporcion'] = genero_pd.groupby('País')['Total'].transform(lambda x: x / x.sum())

pivot = genero_pd.pivot_table(index='País', columns='Género', values='Proporcion', fill_value=0)
pivot['Entropía'] = pivot.apply(lambda row: entropy(row), axis=1)

print(pivot)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

pivot_reset = pivot.reset_index()
plt.figure(figsize=(12, 8))
sns.barplot(x='Entropía', y='País', data=pivot_reset.sort_values('Entropía', ascending=False).head(20), palette='viridis')

plt.title('Top 20 países con mayor diversidad de género (Entropía)', fontsize=16)
plt.xlabel('Entropía', fontsize=12)
plt.ylabel('País', fontsize=12)

plt.show()

# COMMAND ----------

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
 
 
df = df.withColumn("razon_social_normalizada", normalizar_nombre(col("razon_social")))

# COMMAND ----------

@udf(returnType=StringType())
def validar_ruc_simple(ruc):
    return "Ruc bueno" if ruc and ruc.isdigit() and len(ruc) == 11 else "Error"

df_hrc = spark.createDataFrame([("10456789012",), ("abc",)], ["ruc"])
df_hrc = df.withColumn("Rur_Normalizado", validar_ruc_simple(col("ruc")))

# COMMAND ----------

df_hrc.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from delta.`/tmp/ventas`

# COMMAND ----------


df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/FileStore/tables/caso_ventasCorp.csv")
df.show()    

# COMMAND ----------

df.write.format("delta") \
    .partitionBy("ciudad") \
    .mode("overwrite") \
    .save("/tmp/casoventasCorp_hrc")

# COMMAND ----------

df_delta = spark.read.format("delta").load("/tmp/casoventasCorp_hrc")
df_delta.show()
spark.sql("CREATE TABLE ventas_corp_delta_hrc USING DELTA LOCATION '/tmp/casoventasCorp_hrc'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ventas_corp_delta_hrc

# COMMAND ----------

from delta.tables import DeltaTable

delta_ventas = DeltaTable.forPath(spark, "/tmp/casoventasCorp_hrc")

delta_ventas.update(
    condition="categoria = 'Electrónica'",
    set={
        "precio_unitario": "precio_unitario * 1.12",
        "total_venta": "total_venta * 1.12"
    }
)

delta_ventas.update(
    condition="categoria = 'Ropa'",
    set={
        "precio_unitario": "precio_unitario * 1.08",
        "total_venta": "total_venta * 1.08"
    }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/tmp/casoventasCorp_hrc`;
# MAGIC
# MAGIC SELECT venta_id, producto, categoria, precio_unitario, total_venta
# MAGIC FROM delta.`/tmp/casoventasCorp_hrc` VERSION AS OF 0
# MAGIC WHERE categoria IN ('Electrónica', 'Ropa');

# COMMAND ----------

nuevas_ventas = [
    (3, "2024-05-30", 2650, "Zapatillas", "Ropa", 10, 450.0, 4500.0, "Trujillo"),   # Actualiza
    (100, "2025-04-20", 9999, "Smartwatch", "Electrónica", 1, 850.0, 850.0, "Lima")  # Nueva
]

# Crear DataFrame con nombres de columnas
columnas = ["venta_id", "fecha_venta", "cliente_id", "producto", "categoria", "cantidad", "precio_unitario", "total_venta", "ciudad"]
df_nuevas = spark.createDataFrame(nuevas_ventas, columnas)

# COMMAND ----------

from delta.tables import DeltaTable

# Ruta donde está tu tabla Delta
ruta_delta = "/tmp/casoventasCorp_hrc"

# Cargar la tabla Delta existente
delta_ventas = DeltaTable.forPath(spark, ruta_delta)

# MERGE INTO: insertar o actualizar según venta_id
delta_ventas.alias("destino").merge(
    source=df_nuevas.alias("nuevas"),
    condition="destino.venta_id = nuevas.venta_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/tmp/casoventasCorp_hrc`;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# Esquema
schema = StructType([
    StructField("id_producto", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("stock", IntegerType(), True),
    StructField("estado", StringType(), True),
    StructField("canal", StringType(), True)
])

# Cargar CSV sin input_file_name
df_csv = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load("./FileStore/hrc/*20250421.csv")
    .withColumn("llegada", current_timestamp())
)

df_csv.display()

# COMMAND ----------


df_csv.write.format("delta").mode("overwrite").save("/tmp/CasoRetailMax_hrc/stock_audit_hrc")


# COMMAND ----------

spark.read.format("delta").load("/tmp/CasoRetailMax_hrc/stock_audit_hrc").display()

# COMMAND ----------

df_incremental = spark.read.option("header","true").csv("./FileStore/hrc/*20250422.csv").withColumn("llegada", current_timestamp())
df_incremental.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

# Crear una ventana particionada por id_producto, ordenando por mayor stock
window_spec = Window.partitionBy("id_producto").orderBy(desc("stock"))

# Añadir columna de ranking y filtrar solo la primera fila (la de mayor stock)
df_incremental_unico = (
    df_incremental
    .withColumn("rn", row_number().over(window_spec))
    .filter("rn = 1")
    .drop("rn")
)

# COMMAND ----------

from delta.tables import DeltaTable

tabla_maestra = DeltaTable.forPath(spark, "/tmp/CasoRetailMax_hrc/stock_audit_hrc")

# Aplicar MERGE evitando duplicados
tabla_maestra.alias("maestra").merge(
    df_incremental_unico.alias("nuevos"),
    "maestra.id_producto = nuevos.id_producto"
).whenMatchedUpdate(condition="""
    maestra.stock != nuevos.stock OR maestra.estado != nuevos.estado
""", set={
    "stock": "nuevos.stock",
    "estado": "nuevos.estado",
    "nombre": "nuevos.nombre",
    "canal": "nuevos.canal",
    "llegada": "nuevos.llegada"
}).whenNotMatchedInsert(values={
    "id_producto": "nuevos.id_producto",
    "nombre": "nuevos.nombre",
    "stock": "nuevos.stock",
    "estado": "nuevos.estado",
    "canal": "nuevos.canal",
    "llegada": "nuevos.llegada"
}).execute()