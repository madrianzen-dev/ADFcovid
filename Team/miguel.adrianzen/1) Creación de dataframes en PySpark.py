# Databricks notebook source
# MAGIC %md
# MAGIC ## Introducción a la creación de dataframes en PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enlaces y recursos
# MAGIC - [createDataFrame](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html) 
# MAGIC - [Spark Data Types](https://spark.apache.org/docs/3.5.3/sql-ref-datatypes.html)
# MAGIC - [Data Types Class](https://spark.apache.org/docs/3.5.3/api/python/reference/pyspark.sql/data_types.html)

# COMMAND ----------

# MAGIC %md
# MAGIC En este notebook, aprenderá a crear DataFrames a partir de estructuras de datos de Python, especificar esquemas y explorar varios métodos para ver y manipular el esquema.

# COMMAND ----------

# Define a simple data structure
data = [['John','21']]
# Check the type of the data structure
print(type(data))

# COMMAND ----------

# Create a DataFrame from the data structure
df = spark.createDataFrame(data)
# Display the contents of the DataFrame
df.display()

# COMMAND ----------

# Verify the type of the DataFrame
print(type(df))

# COMMAND ----------

# Define a data structure with multiple records
data = [('John', 21), ('Amy',25), ('Anita', 41), ('Rohan', 25), ('Maria', 37)]
# Create a DataFrame from the data structure
df = spark.createDataFrame(data)
# Display the contents of the DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agregar un esquema con nombres de columnas
# MAGIC

# COMMAND ----------

# Define column names
column_names = ['name', 'age']
# Create a DataFrame with the defined column names
df = spark.createDataFrame(data,column_names)
# Display the DataFrame
df.display()

# COMMAND ----------

# Define the schema using SQL-like syntax
schema = 'name string, age int'
# Create a DataFrame using the defined schema
df = spark.createDataFrame(data, schema)
# Display the DataFrame
df.display()
print(df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explorando los tipos de datos de Spark

# COMMAND ----------

# Import PySpark data types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define a schema using StructType and StructField
schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), False)
])

# Create a DataFrame with the custom schema
df = spark.createDataFrame(data, schema=schema)
# Display the DataFrame
df.display()

# COMMAND ----------

# View the schema of the DataFrame
print(df.schema)

# COMMAND ----------

# View the data types of the DataFrame columns
print(df.dtypes)

# COMMAND ----------

# Display the schema using the printSchema() method
df.printSchema()

# COMMAND ----------

# Display basic statistics for the DataFrame
df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creación de un DataFrame a partir de una lista de diccionarios

# COMMAND ----------

# Define data as a list of dictionaries
data = [
    {'name': 'John', 'age': 21},
    {'name': 'Amy', 'age': 25},
    {'name': 'Anita', 'age': 41},
    {'name': 'Rohan', 'age': 25},
    {'name': 'Maria', 'age': 37}
]

# Create a DataFrame from the data
df = spark.createDataFrame(data)
# Display the contents of the DataFrame
df.show()

# COMMAND ----------

# Define a schema for the DataFrame
schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', IntegerType(), False)
])

# Create a DataFrame using the defined schema
df = spark.createDataFrame(data, schema=schema)
# Display the DataFrame
df.show()