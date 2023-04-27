# Databricks notebook source
# MAGIC %run "./includes/includes"

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text('01.start_date', "2021-10-01")
dbutils.widgets.text('02.end_date', "2021-03-01")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.promote_model', 'No')

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load historic data
# MAGIC 
# MAGIC NYC_WEATHER_FILE_PATH

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC 
# MAGIC -- select * 
# MAGIC select DISTINCT main
# MAGIC from historic_weather_info
# MAGIC -- limit 10;
# MAGIC 
# MAGIC /*
# MAGIC id:string ?? why this
# MAGIC dt:string
# MAGIC temp:string
# MAGIC feels_like:string
# MAGIC humidity:string
# MAGIC visibility:string
# MAGIC wind_speed:string
# MAGIC snow_1h:string
# MAGIC rain_1h:string
# MAGIC main:string
# MAGIC pop:string
# MAGIC +------+
# MAGIC |  main|
# MAGIC +------+
# MAGIC |Clouds|
# MAGIC |  Rain|
# MAGIC |  Snow|
# MAGIC | Clear|
# MAGIC +------+
# MAGIC 
# MAGIC // maybess
# MAGIC pressure:string
# MAGIC dew_point:string
# MAGIC uvi:string
# MAGIC wind_deg:string
# MAGIC 
# MAGIC // not so important
# MAGIC clouds:string
# MAGIC description:string
# MAGIC icon:string
# MAGIC loc:string
# MAGIC lat:string
# MAGIC lon:string
# MAGIC timezone:string
# MAGIC timezone_offset:string
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC -- station_id = 5788.13, 66db65aa-0aca-11e7-82f6-3863bb44ef7c
# MAGIC -- show tables;
# MAGIC 
# MAGIC -- bike_station_info
# MAGIC 
# MAGIC -- station_id:string
# MAGIC -- name:string
# MAGIC -- short_name:string //no need for this
# MAGIC -- capacity:long
# MAGIC 
# MAGIC -- has_kiosk:boolean //no need for this
# MAGIC -- station_type:string //no need for this
# MAGIC -- region_id:string
# MAGIC 
# MAGIC -- lat:double //no need for this
# MAGIC -- lon:double //no need for this
# MAGIC -- electric_bike_surcharge_waiver:boolean //no need for this
# MAGIC 
# MAGIC -- legacy_id:string //no need for this
# MAGIC 
# MAGIC -- eightd_has_key_dispenser:boolean //no need for this
# MAGIC -- external_id:string //no need for this
# MAGIC -- rental_methods:array -> element:string
# MAGIC 
# MAGIC -- rental_uris.ios:string //no need for this
# MAGIC -- rental_uris.android:string //no need for this
# MAGIC 
# MAGIC SELECT DISTINCT station_type from bike_station_info 
# MAGIC -- where station_id like '66db65aa-0aca-11e7-82f6-3863bb44ef7c' 
# MAGIC -- limit 5;

# COMMAND ----------

# %sql
# use g13_db;
display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/historic_weather_info/'))

# COMMAND ----------

df = spark.read.format('delta').load('dbfs:/FileStore/tables/G13/historic_weather_info/')
df.select('main').distinct().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC -- station_id = 5788.13, 66db65aa-0aca-11e7-82f6-3863bb44ef7c
# MAGIC -- show tables;
# MAGIC -- SELECT num_scooters_available, num_ebikes_available, num_bikes_available, num_docks_available, num_scooters_unavailable
# MAGIC SELECT *
# MAGIC from station_status
# MAGIC -- where  station_id = '66db65aa-0aca-11e7-82f6-3863bb44ef7c'
# MAGIC limit 5;
# MAGIC 
# MAGIC -- num_ebikes_available:long
# MAGIC -- num_docks_available:long
# MAGIC -- num_scooters_available:double
# MAGIC -- num_bikes_available:long
# MAGIC 
# MAGIC -- num_scooters_unavailable:double
# MAGIC -- num_docks_disabled:long
# MAGIC -- num_bikes_disabled:long
# MAGIC 
# MAGIC -- station_id:string
# MAGIC 
# MAGIC -- //no need for this
# MAGIC -- is_installed:long
# MAGIC -- legacy_id:string //no need for this
# MAGIC -- last_reported:long
# MAGIC -- is_renting:long //no need for this
# MAGIC -- is_returning:long //no need for this
# MAGIC -- eightd_has_available_keys:boolean //is it needed??
# MAGIC -- station_status:string
# MAGIC 
# MAGIC -- drop below as it is null anyway
# MAGIC -- valet.region:string
# MAGIC -- valet.off_dock_capacity:double
# MAGIC -- valet.active:boolean
# MAGIC -- valet.dock_blocked_count:double
# MAGIC -- valet.off_dock_count:double
# MAGIC -- valet.station_id:string
# MAGIC -- valet.valet_revision:double
# MAGIC 
# MAGIC -- SELECT * from bike_station_info WHERE name = 'Lafayette St & E 8 St' LIMIT 10;
# MAGIC -- select * from historic_bike_trips WHERE contains(end_station_id, 190);
# MAGIC -- 1,965 rows for id
# MAGIC -- select count(DISTINCT start_station_name), count(DISTINCT start_station_id) from historic_bike_trips;
# MAGIC -- select count(DISTINCT start_station_id) as a from historic_bike_trips union all SELECT count(DISTINCT end_station_name) as b from historic_bike_trips;
# MAGIC -- SELECT DISTINCT CONCAT(start_station_id, ' ', end_station_id) AS combined_values
# MAGIC -- FROM historic_bike_trips; 10506882, 29585988, 306185
# MAGIC -- SELECT DISTINCT rideable_type FROM historic_bike_trips
# MAGIC -- SELECT count(*) from historic_bike_trips where rideable_type = 'docked_bike';

# COMMAND ----------

# INSIGHTS
# do we find lat/lng important?
# member_casual; notice how these two types of actor have diff behaviour? doubtful
# how is there more station_name than station_id; use station_id for model
# is there a change in station capacity? if so, how about storing density instead of actual count?
# 0.4app - for map use databricks visualization
# total_bike = ebike + bike + scooter
# if is_returning is set to true, it means it accepts unlimited vehicle returns based on below description of the value?
# `Is the station accepting vehicle returns?

# true - Station is accepting vehicle returns. Even if the station is full, if it would otherwise allow vehicle returns, this value MUST be true.
# false - Station is not accepting vehicle returns.

# If the station is temporarily taken out of service and not allowing vehicle returns, this field MUST be set to false.

# If a station becomes inaccessible to users due to road construction or other factors, this field SHOULD be set to false.`

ride_id:string
rideable_type:string
start_station_name:string
start_station_id:string
end_station_name:string
end_station_id:string
member_casual:string
started_at:timestamp
ended_at:timestamp

end_lng:double
end_lat:double
start_lng:double
start_lat:double

# verify update dt
og_df = (spark.read.format('csv').option("inferSchema","True").option('header', 'true')
.load('dbfs:/FileStore/tables/raw/bike_trips/202303_citibike_tripdata.csv'))
display(og_df.orderBy(col('ended_at')))
# display(dbutils.fs.ls(BIKE_TRIP_DATA_PATH))

# COMMAND ----------

# display(dbutils.fs.rm('dbfs:/FileStore/tables/G13/historic_weather/', recurse= True))
display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/historic_weather_info/'))

# COMMAND ----------

import os
# Read data from a CSV file in batch mode
weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(NYC_WEATHER_FILE_PATH)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather_info/"

# clean out directory
if os.path.isdir(output_path):
    dbutils.fs.rm(output_path, recurse = True)

dbutils.fs.mkdirs(output_path)

# define writestream
weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

weather_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_info")

# verify the write
display(weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## load historic data
# MAGIC 
# MAGIC BIKE_TRIP_DATA_PATH

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Get a list of all CSV files in the directory
csv_files = [os.path.join(BIKE_TRIP_DATA_PATH, f.name) for f in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if f.name.endswith('.csv')]

# Define the schema for the DataFrame
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("member_casual", IntegerType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("start_lat", DoubleType(), True),
])

# [ride_id: string, rideable_type: string, started_at: timestamp, ended_at: timestamp, start_station_name: string, start_station_id: string, end_station_name: string, end_station_id: string, start_lat: double, start_lng: double, end_lat: double, end_lng: double, ]


# Create an empty DataFrame
df = spark.createDataFrame([], schema)

# Loop through the CSV files and append them to the DataFrame
for file in csv_files:
    temp_df = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load(file)
#     temp_df = temp_df.withColumn('source_file', input_file_name())
    df = df.unionByName(temp_df)

# COMMAND ----------

# Write the DataFrame to a Delta table
# Write the processed data to a delta table
output_path = GROUP_DATA_PATH + "historic_bike_trips"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

df.write.format("delta").mode("overwrite").save(output_path)

df.write.format("delta").mode("overwrite").saveAsTable("historic_bike_trips")

# COMMAND ----------

# MAGIC %md
# MAGIC # stream live data
# MAGIC ##### BRONZE_NYC_WEATHER_PATH

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, ArrayType

# weather schema
# Define the schema for the DataFrame
# w_schema = StructType([
#     StructField("dt", LongType(), True),
#     StructField("pressure", LongType(), True),
#     StructField("humidity", LongType(), True),
#     StructField("clouds", LongType(), True),
#     StructField("visibility", LongType(), True),
#     StructField("wind_deg", LongType(), True),
#     StructField("wind_gust", DoubleType(), True),
#     StructField("wind_speed", DoubleType(), True),
#     StructField("uvi", DoubleType(), True),
#     StructField("dew_point", DoubleType(), True),
#     StructField("feels_like", DoubleType(), True),
#     StructField("temp", DoubleType(), True),
#     StructField("pop", DoubleType(), True),
#     StructField("rain.1h", DoubleType(), True),
#     StructField("time", DoubleType(), True),
#     StructField("weather", ArrayType(
#         StructType([
#             StructField("description", StringType(), True),
#             StructField("icon", IntegerType(), True),
#             StructField("main", StringType(), True),
#             StructField("id", LongType(), True),
#         ])
#     ), True)    
# ])
# streaming_weather_df.isStreaming
# display(dbutils.fs.rm(output_path, recurse = True))

# COMMAND ----------

import os
# output folder
output_path = GROUP_DATA_PATH + "bronze/nyc_weather"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)
# create readstream
streaming_weather_df = (
    spark.readStream
    .format('delta')
    .option("checkpointLocation", output_path)
    .load(BRONZE_NYC_WEATHER_PATH)
)

weather_query = (
    streaming_weather_df
    .writeStream
    .format("delta").outputMode("append")
    .queryName('bike_status')
    .trigger(processingTime='1 hour')
    .option("checkpointLocation", output_path)
    .option("path", output_path)
    .table('nyc_weather')
)

# Wait for the query to terminate
# weather_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE_STATION_INFO_PATH

# COMMAND ----------

# display(dbutils.fs.rm(GROUP_DATA_PATH + "bronze/bike-info", recurse = True))

# COMMAND ----------

import os
output_path = GROUP_DATA_PATH + "bronze/bike-station-info"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

station_query = (
    spark
    .readStream
    .format('delta')
    .option("checkpointLocation", output_path)
    .load(BRONZE_STATION_INFO_PATH)
#     .writeStream
#     .option("checkpointLocation", output_path)
#     .option("mergeSchema", "true")
#     .option("path", output_path)
#     .outputMode("append")
#     .table('bike_station_info')
)

station_query.isStreaming

# COMMAND ----------

b_query_df = (station_query
              .writeStream
              .format("delta")
              .outputMode("append")  # complete = all the counts should be in the table
              .queryName('bike_info')
              .trigger(processingTime='1 hour')
              .option("checkpointLocation", output_path)
              .option("path", output_path)
              .table('bike_station_info')
#              .start(output_path)
             )

# Wait for the query to terminate
# b_query_df.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### BRONZE_STATION_STATUS_PATH

# COMMAND ----------

import os
status_output_path = GROUP_DATA_PATH + "bronze/bike-status"

if not os.path.isdir(status_output_path):
    dbutils.fs.mkdirs(status_output_path)

stat_df = (
    spark
    .readStream
    .format('delta')
    .option("checkpointLocation", status_output_path)
    .load(BRONZE_STATION_STATUS_PATH)
)

# COMMAND ----------

stat_query = (stat_df
              .writeStream
              .format("delta")
              .outputMode("append")  # complete = all the counts should be in the table
              .queryName('bike_status')
              .trigger(processingTime='1 hour')
              .option("checkpointLocation", status_output_path)
              .option("path", status_output_path)
              .table('station_status')
             )

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC -- show tables;
# MAGIC -- drop table if exists bike_station_info;
# MAGIC select * from bike_station_info limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## store historic data in group path

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/historic_weather'))
# display(dbutils.fs.rm('dbfs:/FileStore/tables/G13/historic_weather', recurse = True))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
