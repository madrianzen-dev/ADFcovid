# Databricks notebook source
#Leer los datos trasnformados
df_result = spark.table("temp_users_promedio")

#Guardarlo en un csv en dbfs
output_path = "/tmp/final_output/promedio_edad_users.csv"

df_result.coalesce(1).write.mode("overwrite").option("header","true").csv(output_path)

print(f"Archivo guardado en : {output_path}")