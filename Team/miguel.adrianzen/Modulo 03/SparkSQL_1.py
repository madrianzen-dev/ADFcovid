# Databricks notebook source
#Spark Sql
#Crear una vista temporal
df = spark.read.csv("/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")

# COMMAND ----------

#Consulta:
spark.sql("select * from clientes limit 5").display()

# COMMAND ----------

spark.sql("select * from clientes where edad > 50").display()

# COMMAND ----------

spark.sql("select nombre,correo from clientes order by id").display()

# COMMAND ----------

#Agrupaciones en spark sql
spark.sql("""
select `País`, count(*) as total_clientes, round(avg(edad),1) as edad_promedio
from clientes
group by `País`
order by total_clientes desc          
""").display()

# COMMAND ----------

#Segmentaciòn 
#¿Cuantos clientes hay por grupo de edad? : Joven menor a 30, Adulto entre 30 y 59, Senior a 60
spark.sql("""
select
    case    
        when edad < 30 then 'Joven'
        when edad between 30 and 59 then 'Adulto'
        else 'Senior'
    END AS Segmento,
    count(*) as total
from clientes
group by Segmento
""").show()


# COMMAND ----------

#¿Cuantos registros se realizaron por mes en el 2023?
#Marco Reategui
spark.sql("""
            Select year(Fecha_Registro) as Mes, count(1)
            from clientes
            where year(Fecha_Registro)='2023'
            group by year(Fecha_Registro)
 
          """).display()

# COMMAND ----------

#juan ponce
spark.sql("""
          select month(fecha_registro) as mes, count(*) as total_registros
          from clientes where year(fecha_registro) = 2023
          group by mes order by mes
""").display()

# COMMAND ----------

#Heberto
from pyspark.sql.functions import to_date, col
from pyspark.sql.functions import year
from pyspark.sql.functions import month, count

df = df.withColumn("Fecha_Registro", to_date(col("Fecha_Registro"), "yyyy-MM-dd"))
df_2023 = df.filter(year("Fecha_Registro") == 2023)
registros_por_mes = df_2023.groupBy(month("Fecha_Registro").alias("Mes")).agg(count("*").alias("Total_Registros"))
registros_por_mes.show()

# COMMAND ----------

#¿Quienes son los 3 clientes mas viejos en cada pais?

# COMMAND ----------

# MAGIC %sql
# MAGIC --hernan
# MAGIC select * from (
# MAGIC select
# MAGIC row_number() over (partition by `País` order by fecha_registro asc) as orden,
# MAGIC s.*
# MAGIC from `clientes` s
# MAGIC ) a where orden< 4

# COMMAND ----------

#Heberto
df_cliente_perfilado = spark.sql("""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY `País` ORDER BY Edad DESC) AS rn
        FROM clientes
    ) sub
    WHERE rn <= 3
""").display()

# COMMAND ----------

#Marco
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
select id,nombre,`país`,edad,
    rank() over (partition by `país` order by edad desc) as rank_edad
from clientes
""").createOrReplaceTempView("ranking_edades")

spark.sql("select * from ranking_edades where rank_edad <=3").display()

# COMMAND ----------

#Cuales son los dominios de correo mas frecuentes?
#Juan Ponce
spark.sql("""
          select substring_index(correo,'@',-1) as dominio, count(*) as total_correos
          from clientes
          group by dominio
          order by total_correos desc
""").display()


# COMMAND ----------

#Heberto
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
select  split(correo,'@')[1] as dominio, count(*) as total_correos
from clientes
group by dominio
order by total_correos desc
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Cuantos clientes se registraron antes y despues del 2024?
# MAGIC --Renan
# MAGIC select count(*)
# MAGIC from clientes s
# MAGIC where year(fecha_registro)> 2024 or year(fecha_registro) < 2024

# COMMAND ----------

#Heberto
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

#Marco
spark.sql("""
            Select sum(case when year(Fecha_Registro)<'2024' then 1 else 0 end) as antes,
                    sum(case when year(Fecha_Registro)>'2024' then 1 else 0 end) as despues
            from clientes
          """).display()

# COMMAND ----------

spark.sql("""
with clasificados as (
    select * ,
        case 
            when year(to_date(Fecha_Registro))<'2024' then 'Antiguo'
            else 'Reciente'
        end as tipo_registro
    from clientes
)          

select tipo_registro, count(*) as total
from clasificados
group by tipo_registro
""").display()

# COMMAND ----------

#Crear un ranking de valor para clientes por pais, basado en edad, y clasificar en "Alto", "Medio" y "Bajo". Si es alto, el ranking de valor es mayor a 0.8, si es medio, es mayor a 0.5 y sino es bajo.

#1. Creamos el percentil de edad por pais ---80%, 70%, 50%...
#2. Luego que tengo el punto 1, lo clasifico en 3 niveles.

#Argentina    Alto   #30





# COMMAND ----------

# MAGIC %sql
# MAGIC with base as (
# MAGIC   select `país`,id,edad,
# MAGIC     percent_rank() over (partition by `país` order by edad desc) as edad_score
# MAGIC   from clientes
# MAGIC ),
# MAGIC segmento as (
# MAGIC   select *,
# MAGIC     case 
# MAGIC       when edad_score > 0.8 then 'Alto'
# MAGIC       when edad_score > 0.5 then 'Medio'
# MAGIC       else 'Bajo'
# MAGIC     end as nivel_valor
# MAGIC   from base
# MAGIC )
# MAGIC
# MAGIC select `país`,nivel_valor,count(*) as total
# MAGIC from segmento
# MAGIC group by `país`, nivel_valor
# MAGIC order by `país`,nivel_valor

# COMMAND ----------

#Detectar y contar correos posiblemente invalidos o sospechosos por patrones: Ejemplo, sin ".com" con dominisos extraños


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC   correo,
# MAGIC     case 
# MAGIC       when correo not like '%@%.%' then 'Invalido'
# MAGIC       when split(Correo,'@')[1] like '%gov%' then 'Gobierno'
# MAGIC       when split(Correo,'@')[1] in ('test.com','demo.org') then 'Ficticio'
# MAGIC       else 'Valido'
# MAGIC     end as tipo_correo
# MAGIC from clientes

# COMMAND ----------

#Analisis por diversidad de genero: Medir cuan balanceado està el genero en cada pais. Usa proporciones y desviaciones.
#Heberto
genero_por_pais = spark.sql("""
    SELECT
         `País`,
        `Género`,
        COUNT(*) AS Total
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

# MAGIC %sql
# MAGIC with resumen AS (
# MAGIC     select 
# MAGIC         `País`,
# MAGIC         `Género`,
# MAGIC         COUNT(*) AS Total
# MAGIC     FROM clientes
# MAGIC     GROUP BY  `País`,`Género`
# MAGIC ),
# MAGIC pivot as (
# MAGIC     select `País`,
# MAGIC         sum(case when `Género`='Masculino' then Total else 0 end) as Hombres,
# MAGIC         sum(case when `Género`='Femenino' then Total else 0 end) as Mujeres,
# MAGIC         sum(total) as total_pais
# MAGIC     from resumen
# MAGIC     group by `País`
# MAGIC )
# MAGIC
# MAGIC select `País`, total_pais,
# MAGIC         round(Hombres/total_pais*100,1) as pct_hombres,
# MAGIC         round(Mujeres/total_pais*100,1) as pct_mujeres
# MAGIC from pivot
# MAGIC order by abs(pct_hombres-pct_mujeres) desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Leer archivos como tablas (sin crear tablas fisicas)
# MAGIC select * from csv.`/FileStore/tables/clientes.csv`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Uso de parquet
# MAGIC select * from parquet.`/dbfs/FileStore/shared_uploads/databricks@databricks.com/clientes.parquet`

# COMMAND ----------

# MAGIC %sql
# MAGIC --Uso de delta 
# MAGIC select * from delta.`/FileStore/shared_uploads/databricks@databricks.com/clientes.delta`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into delta.`/FileStore/shared_uploads/databricks@databricks.com/clientes.delta` as t
# MAGIC using (
# MAGIC   select * from csv.`/FileStore/tables/clientes.csv`) as fuente
# MAGIC on t.id = fuente.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Versionamiento y auditoria para gestiòn de cambios (Delta lake time travel  + comparaciòn de versiones)
# MAGIC select *  from delta.`/FileStore/shared_uploads/databricks@databricks.com/clientes.delta` version as of 0

# COMMAND ----------

#Ejecutar dos queries en paralelo
from concurrent.futures import ThreadPoolExecutor

def consulta1():
    spark.sql("select count(*) from clientes where edad > 50").show()

def consulta2():
    spark.sql("select count(*) from clientes where edad < 30").show()

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(consulta1)
    executor.submit(consulta2)