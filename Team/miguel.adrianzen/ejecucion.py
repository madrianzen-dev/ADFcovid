# Databricks notebook source
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Iniciar sesión Spark (omite esto si ya tienes `spark` en Databricks)
spark = SparkSession.builder.getOrCreate()
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net", "P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==")
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

df_r_p = spark.read.format("csv").option("header", "true").option("delimiter", "\t").option("inferSchema", "true").option("mode", "FAILFAST").load(f"abfss://raw@dbstoragezat11.dfs.core.windows.net/poblacion/"+latest_file.name).withColumn("bronze_load_ts", current_timestamp())
df_r_p.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, split, col

df_r_p = df_r_p.withColumn('age_group', regexp_replace(split(df_r_p['indic_de,geo\\time'], ',')[0], 'PC_', '')).withColumn('country_code', split(df_r_p['indic_de,geo\\time'], ',')[1])
df_r_p = df_r_p.select(col("country_code").alias("country_code"),
                                             col("age_group").alias("age_group"),
                                             col("2019 ").alias("percentage_2019"))
df_r_p.display()

# COMMAND ----------


df_r_p.createOrReplaceTempView("raw_population")
df_raw_population_pivot = spark.sql("
                                    SELECT country_code, 
                                           age_group, 
                                           cast(regexp_replace(percentage_2019, '[a-z]', '') AS decimal(4,2)) AS percentage_2019 
                                           FROM raw_population 
                                           WHERE length(country_code) = 2
                                           ").groupBy("country_code").pivot("age_group").sum("percentage_2019").orderBy("country_code")
df_raw_population_pivot.createOrReplaceTempView("raw_population_pivot")
select_sql = spark.sql("select * from raw_population")
select_sql.display()
select_sql2 = spark.sql("select * from raw_population_pivot")
select_sql2.display()

# COMMAND ----------




archivo = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").option("mode", "FAILFAST").load(f"abfss://raw@dbstoragezat11.dfs.core.windows.net/datacovid/destino/archivo.csv").withColumn("bronze_load_ts", current_timestamp())
archivo.createOrReplaceTempView("dim_archivo")
archivo.display()
query=("""
                 SELECT distinct c.country,
                    c.country_code_3_digit,
                    c.population,
                    p.Y0_14  AS age_group_0_14,
                    p.Y15_24 AS age_group_15_24,
                    p.Y25_49 AS age_group_25_49,
                    p.Y50_64 AS age_group_50_64, 
                    p.Y65_79 AS age_group_65_79,
                    p.Y80_MAX AS age_group_80_max
                FROM raw_population_pivot p
                JOIN dim_archivo c ON p.country_code = c.country_code_2_digit
                ORDER BY country
                """
                )
result2=spark.sql(query)
result2.display()                


# COMMAND ----------

result2.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save("abfss://raw@dbstoragezat11.dfs.core.windows.net/processed/population/result2.csv")