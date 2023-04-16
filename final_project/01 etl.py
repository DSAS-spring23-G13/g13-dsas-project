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

import os
# Read data from a CSV file in batch mode
weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(NYC_WEATHER_FILE_PATH)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

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
# create readstream
streaming_weather_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_NYC_WEATHER_PATH)
)

streaming_weather_df.isStreaming

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "bronze/"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)
    
weather_query = (streaming_weather_df
 .writeStream
 .format("delta")
 .option("checkpointLocation", output_path)
 .option("path", output_path)
 .outputMode("append")  # complete = all the counts should be in the table
 .table('nyc_weather')
#  .start(output_path)
)

# Wait for the query to terminate
weather_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE_STATION_INFO_PATH

# COMMAND ----------

import os
output_path = GROUP_DATA_PATH + "bronze/"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

streaming_station_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_STATION_INFO_PATH)
)

streaming_station_df.isStreaming

# COMMAND ----------

b_query_df = (streaming_station_df
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

# b_query_df.status
b_query_df.stop()

# COMMAND ----------

# bdf=spark.read.format('csv').option("header","True").option("inferSchema","True").load(BIKE_TRIP_DATA_PATH+'202111_citibike_tripdata.csv')
display(df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC -- show tables;
# MAGIC select * from bike_station_info limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## store historic data in group path

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/historic_weather'))
# display(dbutils.fs.rm('dbfs:/FileStore/tables/G13/historic_weather', recurse = True))

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- SHOW DATABASES;
# MAGIC 
# MAGIC use g13_db;
# MAGIC -- drop table if exists weather_csv;
# MAGIC SHOW TABLES;

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
