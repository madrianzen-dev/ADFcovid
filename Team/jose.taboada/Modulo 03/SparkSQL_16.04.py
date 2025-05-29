# Databricks notebook source
#spark SQL
#Crear una vista temporal
df=spark.read.csv("/FileStore/tables/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")


# COMMAND ----------

#Consulta
spark.sql("select * from clientes limit 5").display()

# COMMAND ----------

spark.sql("""
select `País`, count(*) as total_clientes, round(avg(edad),1) as promedio_edad
from clientes
group by `País`
order by `País`, total_clientes desc, promedio_edad desc
limit 5
""").display()

# COMMAND ----------

spark.sql("""
select 
    case 
        when edad <30 then 'JOVEN'
        when edad between 30 and 50 then 'ADULTO'
        when edad is null then 'SIN DATO'
        else 'SENIOR'    
    end as segmento,
    count(*) as total_clientes
from clientes
group by segmento
""").display()

# COMMAND ----------

spark.sql("""
select month(fecha_registro) as mes, count(*) as total_clientes
from clientes
where year(fecha_registro)=2023
group by mes
order by mes asc
""").display()