# Databricks notebook source
# MAGIC %md
# MAGIC # markee

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType
import os

# COMMAND ----------

# MAGIC %run "./includes/includes"

# COMMAND ----------

# MAGIC %md
# MAGIC # BRONZE TABLE
# MAGIC For direct data ingestion, storing historic weather and bike trips data into following delta tables
# MAGIC 1. Historic_weather_data
# MAGIC 2. Historic_Bike_trips
# MAGIC 3. Historic_station_status

# COMMAND ----------

df = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)
df_ = spark.read.format("csv").option("header", "true").option("inferSchema","True").load(NYC_WEATHER_FILE_PATH)
dfa = spark.read.format("delta").load('dbfs:/FileStore/tables/G13/historic_weather_info_v2')

print(df.count())
print(df_.count())
print(dfa.count())

# COMMAND ----------

# bronze weather mar 22/23 - may 2/23
# hist weather nov /21 - mar 10/23
# hist final nov/21 - apr/ 23
# display(dfa.sort(F.desc("dt")).limit(1))
# df_droped = dfa.dropDuplicates(["dt"]) 
# display(df_droped.select("dt").distinct().count())
display(df.limit(5))

# COMMAND ----------

df = spark.read.format("delta").load( 'dbfs:/FileStore/tables/G13/historic_weather_info_v4')
display(df.limit(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading historic weather data using batch stream
# MAGIC <p>removing duplicates by datetime, renaming columns for ease of access</p>

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import pyspark.sql.functions as F

# Define the schema for the DataFrame
weather_schema = StructType([
    StructField("dt", TimestampType(), False),
    StructField("temp", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("clouds", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("pop", DoubleType(), True),
    StructField("snow.1h", DoubleType(), True),
    StructField("main", StringType(), True),
    StructField("rain.1h", DoubleType(), True),
])

# Create an empty DataFrame
weather_df = spark.createDataFrame([], weather_schema)

# Get a list of all CSV files in the directory
csv_files = [os.path.join("dbfs:/FileStore/tables/raw/weather/", f.name) for f in dbutils.fs.ls("dbfs:/FileStore/tables/raw/weather/") if f.name.endswith('.csv')]

# Loop through the CSV files and append them to the DataFrame
for file in csv_files:
    temp_df = (spark
                .read
                .option("header", True)
                .schema(weather_schema)
                .csv(file)
               )
    weather_df = weather_df.unionByName(temp_df)

# store as timestamp
weather_df = weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather_data_final_1/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

# define writestream
weather_df.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE g13_db.historic_weather_data
""")
weather_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_data")

# COMMAND ----------

# rename the "last_name" column to "surname"
final_w_df = (weather_df
        .withColumnRenamed("rain.1h", "rain_1h")
        .withColumnRenamed("snow.1h", "snow_1h")
        .dropDuplicates(["dt"])
)

# display the renamed dataframe
display(final_w_df.count())
display(final_w_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ## [TODO] optimization

# COMMAND ----------

display(df.count())

# COMMAND ----------



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

# Write the DataFrame to a Delta table
# Write the processed data to a delta table
output_path = GROUP_DATA_PATH + "historic_bike_trips"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

df.write.format("delta").mode("overwrite").save(output_path)

df.write.format("delta").mode("overwrite").saveAsTable("historic_bike_trips")

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Define the schema for the DataFrame
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("member_casual", IntegerType(), True),import os
# Read data from a CSV file in batch mode
weather_df = (spark.read
    .format("csv")
    .option("header", "true")
    .load(NYC_WEATHER_FILE_PATH))
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("start_lat", DoubleType(), True),
])

# [ride_id: string, rideable_type: string, started_at: timestamp, ended_at: timestamp, start_station_name: string, start_station_id: string, end_station_name: string, end_station_id: string, start_lat: double, start_lng: double, end_lat: double, end_lng: double, ]
# Define the schema for the Delta table
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])

# Create a Delta table with inferSchema set to true and the defined schema
deltaTable = DeltaTable.createOrReplace(spark, "delta_table",
                                        schema=schema,
                                        path="/path/to/delta_table",
                                        format="delta",
                                        inferSchema=True)


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

import os
# Read data from a CSV file in batch mode
weather_df = (spark.read
    .format("csv")
    .schema()
    .option("header", "true")
    .load(NYC_WEATHER_FILE_PATH))

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
# MAGIC ## Reading historic bike trips data using batch stream

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Get a list of all CSV files in the directory
csv_files = [os.path.join(BIKE_TRIP_DATA_PATH, f.name) for f in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if f.name.endswith('.csv')]

# Define the schema for the DataFrame
bike_schema = StructType([
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

# Create an empty DataFrame
trip_df = spark.createDataFrame([], bike_schema)

# Loop through the CSV files and append them to the DataFrame
for file in csv_files:
    temp_df = spark.read.format('csv').option('header', 'true').csv(file)
    trip_df = trip_df.unionByName(temp_df)

# Write the DataFrame to a Delta table
# Write the processed data to a delta table
output_path = GROUP_DATA_PATH + "historic_bike_trips_final_1/"

# re-create output directory
dbutils.fs.rm(output_path, recurse = True)
dbutils.fs.mkdirs(output_path)

trip_df.write.format("delta").mode("overwrite").save(output_path)

# also recreate delta table
spark.sql("""
drop TABLE g13_db.historic_bike_trips
""")
trip_df.write.format("delta").mode("overwrite").saveAsTable("historic_bike_trips")

# store as timestamp
# weather_df = weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## [TODO] optimization for storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## stream live data

# COMMAND ----------

# MAGIC %md
# MAGIC #### [remove] bike trips

# COMMAND ----------

# create readstream
streaming_trip_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_STATION_INFO_PATH)
)

# streaming_trip_df = (
#     streaming_trip_df
#     .withColumn('last_reported', F.from_unixtime(F.col('last_reported')).cast(TimestampType()))
#     )

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "streaming/bike_trip/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/bike_trip"

#  re-create output directory
dbutils.fs.rm(checkpoint_path, recurse = True)
dbutils.fs.mkdirs(checkpoint_path)

# re do delta table
spark.sql("""
DROP TABLE IF EXISTS g13_db.streaming_bike_trip
""")

trip_query = (
    streaming_trip_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .queryName("trip_traffic")
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", checkpoint_path)
    .option("path", output_path)
    .table('streaming_bike_trip')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### bike status

# COMMAND ----------

# create readstream
streaming_status_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_STATION_STATUS_PATH)
)

streaming_status_df = (
    streaming_status_df
    .withColumn('last_reported', F.from_unixtime(F.col('last_reported')).cast(TimestampType()))
    )

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "streaming/bike_staus/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/bike_staus"

#  re-create output directory
dbutils.fs.rm(checkpoint_path, recurse = True)
dbutils.fs.mkdirs(checkpoint_path)

# re do delta table
spark.sql("""
DROP TABLE IF EXISTS g13_db.streaming_bike_status
""")

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

# MAGIC %md
# MAGIC #### nyc weather

# COMMAND ----------

# create readstream
streaming_weather_df = (
    spark.readStream
    .format('delta')
    .load(BRONZE_NYC_WEATHER_PATH)
)

streaming_weather_df = streaming_weather_df.withColumn('dt', F.from_unixtime(F.col('dt')).cast(TimestampType()))

# COMMAND ----------

# output folder
output_path = GROUP_DATA_PATH + "streaming/nyc_weather/output"
checkpoint_path = GROUP_DATA_PATH + "streaming/nyc_weather"

#  re-create output directory
dbutils.fs.rm(checkpoint_path, recurse = True)
dbutils.fs.mkdirs(checkpoint_path)

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

weather_query.status

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### [todo] check trigger active

# COMMAND ----------

import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, LongType, ArrayType, DateType

status_output_path = GROUP_DATA_PATH + "bronze/bike-status"

if not os.path.isdir(status_output_path):
    dbutils.fs.mkdirs(status_output_path)

stat_df = (
    spark
    .readStream
    .option("schema", schema)
    .option("enforceSchema", "true")
    .format('delta')
    .option("checkpointLocation", status_output_path)
    .load(BRONZE_STATION_STATUS_PATH)
    .writeStream
    .format("delta")
    .outputMode("append")  # complete = all the counts should be in the table
    .queryName('station_status')
    .trigger(processingTime='30 minutes')
    .option("checkpointLocation", status_output_path)
    .option("path", status_output_path)
    .table('bike_status')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## rough work

# COMMAND ----------

# MAGIC %md
# MAGIC ##### schema weather

# COMMAND ----------


# schema
w_schema = StructType([
  StructField("dt", TimestampType(), True),
  StructField("temp", DoubleType(), True),
  StructField("feels_like", DoubleType(), True),
  StructField("pressure", LongType(), True),
  StructField("humidity", LongType(), True),
  StructField("dew_point", DoubleType(), True),
  StructField("uvi", DoubleType(), True),
  StructField("clouds", LongType(), True),
  StructField("visibility", LongType(), True),
  StructField("wind_speed", DoubleType(), True),
  StructField("wind_deg", LongType(), True),
  StructField("wind_gust", DoubleType(), True),
  StructField("weather", ArrayType(
    StructType([
      StructField("description", StringType(), True),
      StructField("icon", StringType(), True),
      StructField("id", LongType(), True),
      StructField("main", StringType(), True)
    ])
  ), True),
  StructField("pop", DoubleType(), True),
  StructField("rain.1h", DoubleType(), True),
  StructField("time", StringType(), True)
])
