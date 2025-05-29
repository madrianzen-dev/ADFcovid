# Databricks notebook source
# Spark SQL
# Crear una vista temporal
df = spark.read.csv("/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")

# COMMAND ----------

#Consulta
spark.sql("SELECT * FROM clientes limit 5").display()

# COMMAND ----------

spark.sql("SELECT * FROM clientes where `país` = 'Afghanistan'").display()

# COMMAND ----------

spark.sql("SELECT * FROM clientes WHERE edad > 50").display()

# COMMAND ----------

spark.sql("SELECT nombre,correo FROM clientes ORDER BY id").count()

# COMMAND ----------

# Agrupaciones en Spark Sql
spark.sql("""
    select 'País', count(*) as total_clientes, round(avg(edad),1) as edad_promedio
    from clientes
    group by 'País'
    order by total_clientes desc
""").display()


# COMMAND ----------

# Segmentacion
# ¿Cuantos clientes hay por grupo de edad? : Joven menor a 30, Adulto entre 30 y 59, Senior mayor a 60
spark.sql("""
          

""").display()


# COMMAND ----------

# ¿Cuantos registros se realizaron por mes en el 2023
spark.sql("""
          select month(fecha_registro) as mes, count(*) as total_registros 
          from clientes where year(fecha_registro) = 2023 
          group by mes order by mes
""").display()

# COMMAND ----------

from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import year
from pyspark.sql.functions import month, count

df = df.withColumn("Fecha_Registro", to_date(col("Fecha_Registro"), "yyyy-MM-dd"))
df_2023 = df.filter(year("Fecha_Registro") == 2023)
registros_por_mes = df_2023.groupBy(month("Fecha_Registro").alias("Mes")).agg(count("*").alias("Total_Registros"))

registros_por_mes.show()

# COMMAND ----------

# ¿Quienes son los 3 clientes mas viejos en cada pais?
spark.sql("""
          select id,nombre, `país`, edad,rank() over (partition by `país` order by edad desc) as ranking
          from clientes
""").createOrReplaceTempView("ranking_edades")

spark.sql("select * from ranking_edades where ranking <= 3").display()

# COMMAND ----------

spark.sql("""
            select * from
            (
            Select  row_number() over (partition by `País`order by Edad desc)as nro,`País`, edad
            From clientes
            ) as edad
            where edad.nro<=3
          """).display()
 

# COMMAND ----------

spark.sql("""
            select * from
            (
            Select  row_number() over (partition by `País`order by Edad desc)as nro,`País`, edad
            From clientes
            ) as edad
            where edad.nro<=3
          """).display()
 

# COMMAND ----------

# Cuales son los dominios de correos mas frecuentes
spark.sql("""
          select substring_index(correo,'@',-1) as dominio, count(*) as total_correos
          from clientes
          group by dominio
          order by total_correos desc
""").display()


# COMMAND ----------

spark.sql("""
    SELECT
        regexp_extract(Correo, '@(.+)', 1) AS Dominio, COUNT(*) AS Total
    FROM clientes
    GROUP BY Dominio
    ORDER BY Total DESC limit 5
""").display()
 

# COMMAND ----------

spark.sql("""
    SELECT split(Correo, '@')[1] AS Dominio, COUNT(*) AS Total 
    FROM clientes GROUP BY Dominio ORDER BY Total DESC limit 5
""").show()
 

# COMMAND ----------

# Cuantos clientes se han registrado antes y despues del 2024
spark.sql("""
          select count(*) as total_clientes, 
                 case when year(fecha_registro) < 2024 then 'Antes 2024' else 'Despues 2024' end as periodo
          from clientes
          group by periodo
""").display()


# COMMAND ----------

spark.sql("""
          with clasificados as (
              select *, case when year(fecha_registro) < 2024 then 'Antes 2024' else 'Despues 2024' end as periodo
              from clientes
          )
          select periodo, count(*) as total_clientes
          from clasificados
          group by periodo
""").display()


# COMMAND ----------

# Crear un ranking de valor para clientes por pais, basado en edad, y clasificar en "Alto", "Medio" y "Bajo". Si es alto, el ranking de valor es mayor a 0.8, si es medio, es mayor a 0.5 y sino es bajo.
# 1. Creamos el percentil de edad por pais --- 80%, 70%, 505 ...
# 2. Luego que tengo el punto 1, lo clasifico en 3 niveles
spark.sql("""
          with base as (
              select `país`, id, edad, 
                     percent_rank() over (partition by `país` order by edad desc) as edad_score
              from clientes
          ),
          segmento as (
              select *, case when edad_score > 0.8 then 'Alto' when edad_score > 0.5 then 'Medio' else 'Bajo' 
              end as nivel_valor
              from base
          )
          select `país`, nivel_valor, count(*) as total_clientes
          from segmento
          group by `país`, nivel_valor
""").display()


# COMMAND ----------

spark.sql("""
    WITH Ranking AS (
        SELECT `país`, Edad,
            ROW_NUMBER() OVER (PARTITION BY `país` ORDER BY Edad DESC) / COUNT(*) OVER (PARTITION BY `país`) AS RankingValor
        FROM clientes
    )
    SELECT 
        `país`,
        CASE 
            WHEN RankingValor > 0.8 THEN 'Alto'
            WHEN RankingValor > 0.5 THEN 'Medio'
            ELSE 'Bajo'
        END AS Clasificacion,
        COUNT(*) AS TotalClientes
    FROM Ranking
    GROUP BY `país`, Clasificacion
    ORDER BY `país`, Clasificacion
""").display()

# COMMAND ----------

# Detectar y contar correos posiblemente invalidos o sospechosos por patrones: Ejemplo, sin .com con dominios extraños
spark.sql("""
          select correo, case when correo not like '%.com' and length(split(correo,'@')[1]) > 3 then 'Sospechoso' else 'Valido' end as correo_valido
          from clientes
          group by correo, correo_valido
""").display()


# COMMAND ----------

spark.sql("""
          select correo, 
          case when correo not like '%@%.%' then 'Invalido'
               when length(split(correo,'@')[1]) > 3 then 'Sospechoso' else 'Valido' 
          end as correo_valido
          from clientes
          group by correo, correo_valido
          order by correo_valido desc
          """).display()


# COMMAND ----------

# Analisis por diversidad de genero. Medir cuan balanceado esta el genero en cada pais. Usa proporciones y desviaciones.
spark.sql("""
    with resumen as (
        SELECT `País`,  `Género`, COUNT(*) AS Total
        FROM clientes
        GROUP BY  `País`,`Género`
    ),
    pivot as (
        select  `País`, 
               sum(case when `Género` = 'Femenino' then 1 else 0 end) as Femenino,
               sum(case when `Género` = 'Masculino' then 1 else 0 end) as Masculino,
               sum(total) as total_pais
               from resumen
               group by `País`

    )
    select `país`, total_clientes, 
            count(*)/sum(count(*)) over (partition by `país`) as prop_genero
    from clientes
    group by `país`, `Género`
""").display()


# COMMAND ----------

genero_por_pais = spark.sql("""
    SELECT `País`,  `Género`, COUNT(*) AS Total
    FROM clientes
    GROUP BY  `País`,`Género`
""")

genero_pd = genero_por_pais.toPandas()
from scipy.stats import entropy
import pandas as pd
 
def calcular_entropia(grupo):
    proporciones = grupo['Total'] / grupo['Total'].sum()
    return entropy(proporciones)
 
entropia_por_pais = genero_pd.groupby('País').apply(calcular_entropia).reset_index(name='Entropía')
 
entropia_por_pais.sort_values(by='Entropía', ascending=False).head()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leer archivos como tablas (sin crear tablas fisicas)
# MAGIC select * from csv.`/FileStore/tables/clientes.csv`
# MAGIC --options (header="true", inferSchema="true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- uso de parquet
# MAGIC select * from parquet.`/FileStore/tables/clientes.parquet`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- uso de delta
# MAGIC select * from delta.`/FileStore/tables/clientes.delta`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into delta.`/FileStore/tables/clientes.delta` as 
# MAGIC using (select * from csv.`/FileStore/tables/clientes.csv`) as new_data
# MAGIC on t_data.id = new_data.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- versionamiento y auditoria para gestion de cambios (Delta lake time travel + comparacion de versiones)
# MAGIC select * from delta.`/FileStore/tables/clientes.delta`
# MAGIC version as of 0
# MAGIC
# MAGIC

# COMMAND ----------

# Ejecutar dos querys en paraelo
from concurrent.futures import ThreadPoolExecutor
 
def ejecutar_query():
    spark.sql("select count(*) from clientes where edad>50").show()

def consulta():
    spark.sql("select count(*) from clientes where edad>50").show()

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.map(ejecutar_query)
    executor.map(consulta)
