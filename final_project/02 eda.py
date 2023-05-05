# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC
# MAGIC -- drop table if exists bike_station_info;?
# MAGIC show tables;
# MAGIC -- select * from bike_station_info limit 10;

# COMMAND ----------

# display(dbutils.fs.ls(f"{GROUP_DATA_PATH}historic_bike_trips"))
from pyspark.sql.functions import date_format, col, dayofmonth

HISTORIC_BIKE_TRIPS = f"{GROUP_DATA_PATH}historic_bike_trips"

test_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)
test_df_1 = (
    test_df
    .filter(col("start_station_name") == GROUP_STATION_ASSIGNMENT)
    .sort(col("started_at").desc())
)
display(test_df_1)

# COMMAND ----------

display(test_df_1.select(col("rideable_type")).distinct())

# COMMAND ----------

display(test_df_1.groupBy(col("rideable_type")).count())

# COMMAND ----------

display(f_df.groupBy(month("started_at")).count())

# COMMAND ----------

# (test_df_1)
from pyspark.sql.functions import unix_timestamp

f_df = test_df_1.withColumn("trip time in mins", ((unix_timestamp(test_df_1.ended_at) - unix_timestamp(test_df_1.started_at)))/ 60)

# COMMAND ----------

display(f_df.sort(col("trip time in mins")).select("end_station_name", "started_at", "ended_at", "trip time in mins" ))

# COMMAND ----------

# display(test_df_1.limit(1))
from pyspark.sql.functions import date_format, col, dayofmonth, month

trend_df = test_df_1.withColumm("started_at_month", month(col("started_at"))).groupBy(month(col("started_at"))).count()
display(trend_df)

# COMMAND ----------

# display(test_df_1.sort(col("started_at")).limit(1))

# COMMAND ----------

test_df = spark.read.format('delta').load('dbfs:/FileStore/tables/G13/historic_bike_trips')
display(test_df)

# COMMAND ----------

# display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/bronze/bike-station-info/'))

# what are monthly trend for station
df_2023_01 = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load('dbfs:/FileStore/tables/raw/bike_trips/202301_citibike_tripdata.csv')

from pyspark.sql.functions import date_format, col, dayofmonth

filter_df_ = df_2023_01.filter(col("start_station_name") == GROUP_STATION_ASSIGNMENT)
# display(filter_df.head(10))
final_df_ = filter_df_.groupBy(dayofmonth(col("started_at")).alias("day")).count()
display(final_df_)

# COMMAND ----------

from pyspark.sql.functions import date_format, col, dayofmonth

filter_df = df_2023_01.filter(col("start_station_name") == GROUP_STATION_ASSIGNMENT)
# display(filter_df.head(10))
final_df = filter_df.groupBy(dayofmonth(col("started_at")).alias("day")).count()
display(final_df)

# COMMAND ----------

from pyspark.sql.functions import date_format, col, dayofmonth

filter_df = df_2022_12.filter(col("start_station_name") == GROUP_STATION_ASSIGNMENT)
display(filter_df.head(10))

# COMMAND ----------

final_df = filter_df.groupBy(dayofmonth(col("started_at")).alias("day")).count()
display(final_df)

# COMMAND ----------

df_2022_12 = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load('dbfs:/FileStore/tables/raw/bike_trips/202212_citibike_tripdata.csv')
# display(df_2022_11.count())

# Define the date range
df = df_2022_12.alias("df")
# start_date = "2023-01-01"
# end_date = "2023-01-31"

from pyspark.sql.functions import date_format, col, dayofmonth

# Convert the dates to timestamps
# start_ts = pd.Timestamp(start_date)
# end_ts = pd.Timestamp(end_date)

df_dated = (df
#             .filter((col("started_at") >= start_ts) & (col("ended_at") <= end_ts))
            .groupBy(dayofmonth(col("started_at")).alias("day")).count()
#             .withColumn("started_at", date_format("started_at", "dd/MM/yy"))
#             .withColumn("ended_at", date_format("ended_at", "dd/MM/yy"))

           )
display(df_dated)

# COMMAND ----------

df_2022_11 = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load('dbfs:/FileStore/tables/raw/bike_trips/202211_citibike_tripdata.csv')
# display(df_2022_11.count())

# Define the date range
df = df_2022_11.alias("df")
start_date = "2023-01-01"
end_date = "2023-01-31"

from pyspark.sql.functions import date_format, col, dayofmonth

# Convert the dates to timestamps
# start_ts = pd.Timestamp(start_date)
# end_ts = pd.Timestamp(end_date)

df_dated = (df
#             .filter((col("started_at") >= start_ts) & (col("ended_at") <= end_ts))
            .groupBy(dayofmonth(col("started_at")).alias("day")).count()
#             .withColumn("started_at", date_format("started_at", "dd/MM/yy"))
#             .withColumn("ended_at", date_format("ended_at", "dd/MM/yy"))

           )
display(df_dated)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_2023_2 = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load('dbfs:/FileStore/tables/raw/bike_trips/202302_citibike_tripdata.csv')
display(df_2023_2.count())

# COMMAND ----------

df_2023_1 = spark.read.format('csv').option("inferSchema","True").option('header', 'true').load('dbfs:/FileStore/tables/raw/bike_trips/202301_citibike_tripdata.csv')
display(df_2023_1.count())
# display(dbutils.fs.ls('BIKE_TRIP_DATA_PATH'))
# spark.era

# COMMAND ----------

from pyspark.sql.functions import date_format, col, dayofmonth

# Define the date range
df = df_2023_1.alias("df")
start_date = "2023-01-01"
end_date = "2023-01-31"

# Convert the dates to timestamps
# start_ts = pd.Timestamp(start_date)
# end_ts = pd.Timestamp(end_date)

df_dated = (df
#             .filter((col("started_at") >= start_ts) & (col("ended_at") <= end_ts))
            .groupBy(dayofmonth(col("started_at")).alias("day")).count()
#             .withColumn("started_at", date_format("started_at", "dd/MM/yy"))
#             .withColumn("ended_at", date_format("ended_at", "dd/MM/yy"))

           )
display(df_dated)

# COMMAND ----------

from pyspark.sql.functions import date_format, col, dayofmonth

# Define the date range
df = df_2023_2.alias("df")
start_date = "2023-01-01"
end_date = "2023-01-31"

# Convert the dates to timestamps
# start_ts = pd.Timestamp(start_date)
# end_ts = pd.Timestamp(end_date)

df_dated = (df
#             .filter((col("started_at") >= start_ts) & (col("ended_at") <= end_ts))
            .groupBy(dayofmonth(col("started_at")).alias("day")).count()
#             .withColumn("started_at", date_format("started_at", "dd/MM/yy"))
#             .withColumn("ended_at", date_format("ended_at", "dd/MM/yy"))

           )
display(df_dated)

# COMMAND ----------

# df = spark.read.format('delta').load(f'{GROUP_DATA_PATH}historic_bike_trips')
display(df)

# COMMAND ----------

# DataFrame[ride_id: string, rideable_type: string, start_station_name: string, start_station_id: string, end_station_name: string, end_station_id: string, member_casual: string, started_at: timestamp, ended_at: timestamp, end_lng: double, end_lat: double, start_lng: double, start_lat: double]

from pyspark.sql.functions import date_format

# Define the date range
start_date = "2023-01-01"
end_date = "2023-01-20"

# Convert the dates to timestamps
start_ts = pd.Timestamp(start_date)
end_ts = pd.Timestamp(end_date)

df_dated = (df
            .filter((col("started_at") >= start_ts) & (col("ended_at") <= end_ts))
            .withColumn("started_at", date_format("started_at", "MMMM dd, yyyy"))
            .withColumn("ended_at", date_format("ended_at", "MMMM dd, yyyy"))
           )
display(df_dated)

# COMMAND ----------

# Import the necessary libraries
from pyspark.sql.functions import col, desc, mode

# Find the most frequent value in each column
modes = df.agg(*(mode(c).alias(c) for c in df.columns))

# Sort the DataFrame by the count of each value
sorted_df = df.orderBy(*[desc(c) for c in modes.columns])

# Show the sorted DataFrame
sorted_df.show()


# COMMAND ----------

display(sorted_df)
# most frequently departed station
# count_stn = df.groupBy('start_station_name').count()
# display(count_stn)

# COMMAND ----------

# Import the necessary libraries
from pyspark.sql.functions import col

display(count_stn.filter(col('start_station_name') ==  GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

old_bike_info_fs = [file.path for file in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if file.name.endswith("_citibike_tripdata.csv")]

# old_bike_info_fs

# COMMAND ----------

# lets start with last years trend
oldb_df = spark.read.format("csv").option("header", "true").load('dbfs:/FileStore/tables/raw/bike_trips/202301_citibike_tripdata.csv')
display(oldb_df)

# COMMAND ----------

### what is percent of 

# COMMAND ----------

# unique rideable
unique_values = [row.rideable_type for row in oldb_df.select("rideable_type").distinct().collect()]
# unique_values # ['docked_bike', 'classic_bike', 'electric_bike']

# how about some commonly start start_station_name

# COMMAND ----------

dbutils.fs.ls(BIKE_TRIP_DATA_PATH)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
