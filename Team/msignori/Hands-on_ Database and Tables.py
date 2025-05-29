# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE TABLE managed_default
# MAGIC (
# MAGIC   width INT,lenght INT, height INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO managed_default
# MAGIC VALUES
# MAGIC (1,2,3)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC (
# MAGIC   width INT,
# MAGIC   length INT,
# MAGIC   heigth INT
# MAGIC )
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';
# MAGIC
# MAGIC INSERT INTO external_default
# MAGIC VALUES
# MAGIC (3 INT,2 INT,1 INT);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_default

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;
# MAGIC     
# MAGIC CREATE TABLE managed_new_default
# MAGIC (width INT,lenght INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES
# MAGIC (1,2,3);
# MAGIC
# MAGIC
# MAGIC CREATE TABLE external_new_default
# MAGIC (
# MAGIC   width INT,
# MAGIC   length INT,
# MAGIC   heigth INT
# MAGIC )
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES
# MAGIC (3 INT,2 INT,1 INT);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_new_default;
# MAGIC     
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default.db/managed_new_default';

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default';