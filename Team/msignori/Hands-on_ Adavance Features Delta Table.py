# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC USE SCHEMA default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v4

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO VERSION AS OF 6

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC ZORDER BY id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'