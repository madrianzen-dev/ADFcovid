# Databricks notebook source
import dlt
from pyspark.sql.functions import *


@dlt.table(comment="raw table that contains NYC taxi trips")
@dlt.expect_or_drop("valid_distance", "trip_distance > 0.0")
def taxi_raw_records():
    return spark.readStream.table("samples.nyctaxi.trips")


@dlt.table(comment="total fare amount per week")
def total_fare_amount_by_week():
    return (
        dlt.read("taxi_raw_records")
        .groupBy(date_trunc("week", col("tpep_pickup_datetime")).alias("week"))
        .agg(sum("fare_amount").alias("total_amount"))
    )


@dlt.table(comment="max distance recorded by week")
def max_distance_by_week():
    return (
        dlt.read("taxi_raw_records")
        .groupBy(date_trunc("week", col("tpep_pickup_datetime")).alias("week"))
        .agg(max("trip_distance").alias("max_distance"))
    )