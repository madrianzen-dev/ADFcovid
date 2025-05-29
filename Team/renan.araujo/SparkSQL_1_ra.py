# Databricks notebook source
df = spark.read.csv("/dbfs/clientes.csv",header=True,inferSchema=True)
df.createOrReplaceTempView("clientes")