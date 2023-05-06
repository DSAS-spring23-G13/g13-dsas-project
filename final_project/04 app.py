# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

import folium
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType
import os
from pyspark.sql.functions import to_date,col, from_unixtime, count, coalesce, year, month, concat_ws, date_format, avg

# station_id = 5788.13
# stn id = 66db65aa-0aca-11e7-82f6-3863bb44ef7c
# lon = -73.99102628231049
# lat = 40.73020660529954

# Replace 'your_station_id' with the actual station_id you're interested in
# your_station_id = 'your_station_id'
bike_station_df = spark.read.format('delta').load(G13_BRONZE_BIKE_TRIP)

# Find the latitude and longitude of your station
station_info = bike_station_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).take(1)[0]
station_latitude = station_info.start_lat
station_longitude = station_info.start_lng

# Create a Folium map centered at the station's location with the default "OpenStreetMap" map tile
m = folium.Map(location=[station_latitude, station_longitude], zoom_start=48)

# Create a custom icon for the marker with the color set to 'red'
icon = folium.Icon(icon='bicycle', prefix='fa', color='red')

# Add a marker for your station with the custom icon and popup text
folium.Marker([station_latitude, station_longitude], popup='Station Name - Lafayette St & E 8 St', icon=icon).add_to(m)

# Display the map
m

# COMMAND ----------

station_latitude

# COMMAND ----------

bike_station_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).take(1)[0]

# COMMAND ----------

streaming_status_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_STATION_STATUS_PATH)
)

streaming_status_df = (
    streaming_status_df
    .withColumn('last_reported', F.from_unixtime(F.col('last_reported')).cast(TimestampType()))
    )

# output folder
output_path = GROUP_DATA_PATH + "streaming/bike_staus/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/bike_staus"

status_query = (
    streaming_status_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .queryName("status_traffic")
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
    )

# COMMAND ----------

bike_status_df = (
    spark
    .read.format("delta").load(GROUP_DATA_PATH + "streaming/bike_staus/output")
    .filter(F.col("station_id") == "66db65aa-0aca-11e7-82f6-3863bb44ef7c").sort("last_reported")
    )
# display(bike_station_df.tail(1))
# display(
#     bike_status_df.sort(F.desc("last_reported"))
#     .withColumn("capacity", (F.col("num_docks_available") + F.col("num_docks_disabled") + F.col("num_bikes_available")))
#     .select("capacity")
#     .sort(F.desc("capacity"))
#     .limit(20)
#     ) # 4050854 4050854 4054668
# display(bike_status_df.count())

# COMMAND ----------

# create readstream
streaming_weather_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_NYC_WEATHER_PATH)
)

streaming_weather_df = streaming_weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# output folder
output_path = GROUP_DATA_PATH + "streaming/nyc_weather/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/nyc_weather"

#  re-create output directory
# dbutils.fs.rm(checkpoint_path, recurse = True)
# dbutils.fs.mkdirs(checkpoint_path)
# dbutils.fs.mkdirs(output_path)

# re do delta table
spark.sql("""
DROP TABLE IF EXISTS g13_db.streaming_nyc_weather
""")

weather_query = (
    streaming_weather_df
    .writeStream
    .outputMode("append")
    .format("delta")
    .queryName("nyc_traffic")
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", checkpoint_path)
    .start(output_path)
)

# COMMAND ----------

ny_station_df = spark.read.format("delta").load(GROUP_DATA_PATH + "streaming/nyc_weather/output")
# display(ny_station_df.sort(F.desc("dt")).limit(1))
weather_now = ny_station_df.sort(F.desc("dt")).take(1)[0]
# print("Current weather")
# print(f"temp: {weather_now.temp} K, looks like: {weather_now.weather[0]['main']}, cloud%: {weather_now.clouds}, humidity%: {weather_now.humidity}, precip: {weather_now['rain.1h']}")

# COMMAND ----------

displayHTML(f"""
<p>CURRENT timestamp: {datetime.datetime.now()}</p>
<p>station name: {GROUP_STATION_ASSIGNMENT}</p>
<p>current weather: temp: {weather_now.temp} K, looks like: {weather_now.weather[0]['main']}, cloud%: {weather_now.clouds}, humidity%: {weather_now.humidity}, precip: {weather_now['rain.1h']}</p>
<p>total docks: 105</p>
""")
