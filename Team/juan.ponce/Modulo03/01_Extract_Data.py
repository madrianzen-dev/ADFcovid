# Databricks notebook source
data= [
    ("Carlos",34,"Lima"),
    ("Pedro",45,"Arequipa"),
    ("Juan",29,"Trujillo"),
    ("Cesar",67,"Chimbote"),
    ("Mario",32,"Tumbes")
]

columns = ["name","age","city"]
df = spark.createDataFrame(data,columns)
df.write.mode("overwrite").saveAsTable("temp_users_raw")
