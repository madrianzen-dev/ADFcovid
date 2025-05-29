# Databricks notebook source
spark.conf.set("fs.azure.account.key.dbstoragezat11.dfs.core.windows.net","P/k6TjBoeX8epL95UBs3cL4WtOOUUwGOpbgCdb5U1jniV7WBfCkyjluINA0xRSB63YE4G5iT4YZl+AStx3W6Ug==")
spark = SparkSession.builder.getOrCreate()
folder="abfss://rawmad@dbstoragezat11.dfs.core.windows.net/procesado/"

files = dbutils.fs.ls(folder)
 
print(files)
procesado_files = [f for f in files if f.path.endswith(".csv")]
ultimo_Archivo = sorted(procesado_files, key=lambda x: x.modificationTime, reverse= True)[0]
print(ultimo_Archivo.name)
df_casos_muertes_europa = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema","true").load(folder+ultimo_Archivo.name)
#df_casos_muertes_europa.display()
df_casos_muertes_europa.createOrReplaceTempView("raw_casos_muertes_europa")
query=("""
       select distinct Pais,
              procesado.Codigo_3_dig,
              procesado.Codigo_2_dig,
              procesado.Poblacion,
              pivot.Y0_14  ,
              pivot.Y15_24 ,
              pivot.Y25_49 ,
              pivot.Y50_64 ,
              pivot.Y65_79 ,
              pivot.Y80_MAX
         from raw_casos_muertes_europa procesado
    inner join raw_pob_x_edad_pivot pivot on
              procesado.Codigo_2_dig=pivot.country_code
     order by Pais
       """)
df_procesado=spark.sql(query)
#df_procesado.display()
df_procesado.write.format("csv").option("header", "true").option("delimiter", ",").mode("overwrite").save(folder+"/procesofinal/paises_procesado.csv")