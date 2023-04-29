# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, from_unixtime

HISTORIC_BIKE_TRIPS = f"dbfs:/FileStore/tables/G13/historic_bike_trips/"

historic_bike_trips_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)
historic_bike_trips_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))

# Read the weather data
nyc_weather_df = spark.read.csv(
    "dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv",
    header=True,
    inferSchema=True
)
# Convert the 'dt' column from Unix timestamp to a date type column
nyc_weather_df = nyc_weather_df.withColumn("date", to_date(from_unixtime("dt")))

bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"
bike_status_df = spark.read.format('delta').load(bike_status)

bike_station = f"dbfs:/FileStore/tables/G13/bronze/bike-station-info/"
bike_station_df = spark.read.format('delta').load(bike_station)

# COMMAND ----------

bike_status_df.head(3)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, from_unixtime

# Extract the date and hour from the 'started_at' column
historic_bike_trips_df = historic_bike_trips_df.withColumn("date", F.to_date("started_at")).withColumn("hour", F.hour("started_at"))

# Aggregate bike trips data by date and hour
bike_trips_agg = historic_bike_trips_df.groupBy("date", "hour").agg(
    F.count("ride_id").alias("trip_count")
)

# Aggregate weather data by date and hour
nyc_weather_df = nyc_weather_df.withColumn("hour", F.hour(from_unixtime("dt")))
weather_hourly_agg = nyc_weather_df.groupBy("date", "hour").agg(
    F.avg("temp").alias("avg_temperature"),
    F.avg("rain_1h").alias("avg_precipitation"),
    F.avg("wind_speed").alias("avg_wind_speed")
)

# Join the aggregated bike trips and weather data on the 'date' and 'hour' columns
merged_df = bike_trips_agg.join(weather_hourly_agg, on=["date", "hour"])

# COMMAND ----------

display(merged_df.head(3))

# COMMAND ----------

display(bike_status_df.head(3))

# COMMAND ----------

GROUP_STATION_ASSIGNMENT

# COMMAND ----------

# Filter bike_status_df to only include the station you're interested in
bike_status_filtered = bike_status_df.filter(F.col("station_id") == GROUP_STATION_ASSIGNMENT)

# COMMAND ----------

display(bike_status_filtered.head(3))

# COMMAND ----------

# Calculate the net bike change for the station
bike_status_filtered = bike_status_filtered.withColumn("net_bike_change", F.col("num_bikes_available") - F.col("num_bikes_disabled"))

# COMMAND ----------

# Join the resulting dataframe with bike_station_df on the station_id column
bike_status_station_joined = bike_status_filtered.join(bike_station_df, on="station_id")

# COMMAND ----------

# Extract the date and hour from the 'last_reported' column (Unix timestamp)
bike_status_station_joined = bike_status_station_joined.withColumn("date", F.to_date(from_unixtime("last_reported"))).withColumn("hour", F.hour(from_unixtime("last_reported")))

# COMMAND ----------

# Join this dataframe with merged_df on the date and hour columns
final_df = merged_df.join(bike_status_station_joined, on=["date", "hour"])

# COMMAND ----------

final_df.head(3)

# COMMAND ----------

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

final_df.head(3)

# COMMAND ----------

print(final_df.describe())

# COMMAND ----------

print(final_df.isnull().sum())

# COMMAND ----------


