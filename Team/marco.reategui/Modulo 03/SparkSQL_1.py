# Databricks notebook source
#Spark SQL
#Crear una vista Temporal

df = spark.read.csv("dbfs:/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")

# COMMAND ----------

#CONSULTAS:
spark.sql("SELECT * FROM clientes").display()

# COMMAND ----------

#Agrupaciones Spark SQL

spark.sql("""
          Select {País},count(1) as total_clientes, round(avg(edad),1) as edad_promedio
          from clientes 
          group by {País}
          order by total_clientes desc
          """).display()

# COMMAND ----------


# Segmentacion
# ¿Cuantos registros se realizaron por mes en el 2023?

spark.sql("""
            Select month(Fecha_Registro) as Mes, count(1)
            from clientes
            where year(Fecha_Registro)='2023'
            group by month(Fecha_Registro)
            order by 1 desc

          """).display()



# COMMAND ----------

# Quienes son los 3 clientes mas viejos en cada pais.

spark.sql("""
            select * from
            (
            Select  row_number() over (partition by `País`order by Edad desc)as nro,`País`, edad
            From clientes
            ) as edad
            where edad.nro<=3
          """).display()

# COMMAND ----------

#Cuales los dominios de correo mas frecuentes

spark.sql("""
            select substring_index(correo,'@',-1) as dom, count(*) as total
          from clientes
          group by dom
          order by total desc
          """).display()

# COMMAND ----------

#cuantos clientes se registraron antes y despues

spark.sql("""
            Select sum(case when year(Fecha_Registro)<'2024' then 1 else 0 end) as antes,
                    sum(case when year(Fecha_Registro)>='2024' then 1 else 0 end) as despues
            from clientes
          """).display()

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

#Crear un ranking de valor clientes por pais, basados en edad, y clasificar en "Alto", "Medio","Bajo"

spark.sql("""
          with data as (
          select `País` as pais, edad, percent_rank() over (partition by `País` order by edad desc) as ranking
          from clientes
          ),
          segmento as (
                Select pais, edad, 
                    case when ranking>0.8 then 'Alto' 
                         when ranking>0.5 then 'Medio' 
                    else 'Bajo' 
                    end as valor
                from data
          )

          select pais, count(1), valor
          from segmento
          group by pais, valor
          order by pais

          """).display()
          

# COMMAND ----------

# MAGIC %sql
# MAGIC Select correo, 
# MAGIC       case when correo not like '%@%' then 'Invalido'
# MAGIC            when split(correo,'@')[1] like '%gmail%' then 'Gmail'  
# MAGIC from clientes
# MAGIC
# MAGIC

# COMMAND ----------

#Analisis por diversidad de genero: Medir cuan balanceado esta el genero en cada pais, Usa proporciones y deviaciones

spark.sql("""
                with data as(
                    select  `País` as pais,`Género` as genero, count(1)as can 
                    from clientes 
                    group by  `País`,`Género`
                ),
                pivot as (
                    select pais, 
                        sum(case when genero='Femenino' then can else 0 end) as femenino,
                        sum(case when genero='Masculino' then can else 0 end) as masculino,
                        sum(can) as total
                    from data
                    group by pais
                ) 

                select pais, total,
                        round(femenino/total*1,1) as porc_femenino,
                        round(masculino/total*1,1) as porc_masculino
                from pivot
                order by abs(porc_femenino-porc_masculino)

          """).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Leer archivos como tablas (sin crear tablas fisicas)
# MAGIC
# MAGIC select * from csv.`/FileStore/tables/clientes.csv`
# MAGIC --options(header="true", inferSchema="true")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from parquet.`/dbfs/filestore/shared_uploads/databricks@databricks.com/clientes.parquet`;
# MAGIC select * from delta.`/dbfs/filestore/shared_uploads/databricks@databricks.com/clientes.delta`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into delta.`/dbfs/filestore/shared_uploads/databricks@databricks.com/clientes.delta` as t
# MAGIC using (select * from csv.`/FileStore/tables/clientes.csv`) as fuente
# MAGIC on t.id = fuente.id
# MAGIC when matched then update set *
# MAGIC when not matched then insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --- Versionamiento y auditoria para gestion de cambios (delta lake time travel + comparacion de versiones)
# MAGIC select * from delta.`/dbfs/filestore/shared_uploads/databricks@databricks.com/clientes.delta`
# MAGIC version as of 0

# COMMAND ----------

# Ejecutar dos queris en paralelo

from concurrent.futures import ThreadPoolExecutor

def consulta1(): spark.sql(""" select count(1) from clientes where edad>50  """).show()
def consulta2(): spark.sql(""" select count(1) from clientes where edad<30  """).show()

with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(consulta1)
    executor.submit(consulta2)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) from clientes where edad>50 ;
# MAGIC select count(1) from clientes where edad<30 