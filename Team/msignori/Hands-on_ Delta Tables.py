# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE default.employees
# MAGIC (
# MAGIC   id INT , 
# MAGIC   name STRING,
# MAGIC   salary DOUBLE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.employees
# MAGIC VALUES
# MAGIC   (1,"Adam",3500.0),
# MAGIC   (2,"Sarah",4000.0)
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO default.employees
# MAGIC VALUES
# MAGIC   (3,"Jhon",32500.0),
# MAGIC   (4,"Tomas",41000.0)
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO default.employees
# MAGIC VALUES
# MAGIC
# MAGIC   (5,"Will",3500.0),
# MAGIC   (6,"Rox",4000.0)
# MAGIC ;
# MAGIC
# MAGIC INSERT INTO default.employees
# MAGIC VALUES
# MAGIC
# MAGIC   (7,"Aldo",3500.0),
# MAGIC   (8,"Connor",4000.0)
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees
# MAGIC SET salary = salary * 1.1
# MAGIC WHERE name LIKE 'A%'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000006.json'