-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE taxi_raw_records_mad (
  CONSTRAINT valid_distance EXPECT (trip_distance > 0.0) ON VIOLATION DROP ROW
) AS
SELECT * FROM STREAM(zatdev111db.bronze.taxi_raw_sql2)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE total_fare_amount_by_week_mad AS
SELECT date_trunc("week", tpep_pickup_datetime) as week,
  SUM(fare_amount) as total_monto
FROM live.taxi_raw_records_mad 
GROUP BY week

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE max_distance_by_week_mad AS
SELECT date_trunc("week", tpep_pickup_datetime) as week,
  MAX(trip_distance) as max_distance
FROM live.taxi_raw_records_mad
GROUP BY week