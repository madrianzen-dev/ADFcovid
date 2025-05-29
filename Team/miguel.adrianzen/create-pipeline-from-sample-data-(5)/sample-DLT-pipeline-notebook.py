# Databricks notebook source
import dlt
from pyspark.sql.functions import *


@dlt.table(comment="raw table that contains NYC taxi trips")
@dlt.expect_or_drop("valid_distance", "trip_distance > 0.0")
def taxi_raw_records_py():
    return spark.readStream.table("zatdev111db.bronze.taxi_raw_sql2")


@dlt.table(comment="total fare amount per week")
def total_fare_amount_by_week_py():
    return (
        dlt.read("taxi_raw_records_py")
        .groupBy(date_trunc("week", col("tpep_pickup_datetime")).alias("semana"))
        .agg(sum("fare_amount").alias("monto_total"))
    )


@dlt.table(comment="max distance recorded by week")
def max_distance_by_week_py():
    return (
        dlt.read("taxi_raw_records_py")
        .groupBy(date_trunc("week", col("tpep_pickup_datetime")).alias("semana"))
        .agg(min("trip_distance").alias("minima_distancia"))
    )