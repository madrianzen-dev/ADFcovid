# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/clientes.csv"
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "temp_cli"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `temp_cli` limit 3

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select 
# MAGIC case when edad>30 then 'Joven'
# MAGIC   when edad between 20 and 30 then 'Adulto'
# MAGIC   end segmento,
# MAGIC   count(*) as total
# MAGIC  from `temp_cli`
# MAGIC  group by case when edad>30 then 'Joven'
# MAGIC   when edad between 20 and 30 then 'Adulto'
# MAGIC   end 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC EXTRACT(MONTH FROM fecha_registro) AS mes,
# MAGIC count(*) as ctd
# MAGIC  from `temp_cli`
# MAGIC  where year(fecha_registro)=2023 
# MAGIC  group by EXTRACT(MONTH FROM fecha_registro)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from (
# MAGIC select 
# MAGIC row_number() over (partition by `País` order by fecha_registro asc) as orden,
# MAGIC s.*
# MAGIC from `temp_cli` s
# MAGIC ) a where orden< 4

# COMMAND ----------

# MAGIC %sql
# MAGIC select *--count(*)
# MAGIC from `temp_cli` s 
# MAGIC where year(fecha_registro)> 2024 or year(fecha_registro) < 2024

# COMMAND ----------

# MAGIC %sql
# MAGIC with cliente as 
# MAGIC (SELECT 
# MAGIC     `País`,
# MAGIC     PERCENTILE_CONT(0.5) 
# MAGIC         WITHIN GROUP (ORDER BY edad) AS percentiles
# MAGIC FROM 
# MAGIC     `temp_cli`
# MAGIC GROUP BY 
# MAGIC     `País`)
# MAGIC select * from cliente limit 2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM `temp_cli`
# MAGIC WHERE correo NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$';