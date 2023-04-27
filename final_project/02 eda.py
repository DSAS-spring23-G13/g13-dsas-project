# Databricks notebook source
# MAGIC %run ./includes/includes

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
print("YOUR CODE HERE...")

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/bronze'))

# COMMAND ----------

NYC_WEATHER_FILE_PATH
BIKE_TRIP_DATA_PATH
BRONZE_STATION_INFO_PATH
BRONZE_STATION_STATUS_PATH
BRONZE_NYC_WEATHER_PATH

# COMMAND ----------

from pyspark.sql.functions import to_date,col
from pyspark.sql.functions import year, month, concat_ws, date_format

historic_bike_trips = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter("start_station_name == 'Lafayette St & E 8 St' or  end_station_name == 'Lafayette St & E 8 St'").withColumn("start_date_column", to_date(col('started_at'))).withColumn("year_month", year(col('start_date_column'))*100 + month(col('start_date_column'))).withColumn("day_of_the_week", date_format(col("started_at"),"E"))
display(historic_bike_trips)

# COMMAND ----------

year_month_counts = historic_bike_trips.groupBy("year_month").count().orderBy("year_month")
days_order = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
day_of_week_counts = historic_bike_trips.groupBy("day_of_the_week").count()

# COMMAND ----------

import matplotlib.pyplot as plt

x = year_month_counts.select("year_month").rdd.flatMap(lambda x: x).collect()
x = [str(d)[:4] + "-" + str(d)[4:] for d in x]
y = year_month_counts.select("count").rdd.flatMap(lambda x: x).collect()

plt.figure(figsize=(10, 5))
plt.bar(x, y)
plt.xticks(rotation=45)
plt.xlabel("Year-Month")
plt.ylabel("Bike Trip Count")
plt.title("Record Bike Trip Count by Year-Month")
plt.show()

# COMMAND ----------

x = day_of_week_counts.select("day_of_the_week").rdd.flatMap(lambda x: x).collect()
y = day_of_week_counts.select("count").rdd.flatMap(lambda x: x).collect()

plt.figure(figsize=(10, 5))
plt.bar(x, y)
plt.xticks(rotation=45)
plt.xlabel("Day of the Week")
plt.ylabel("Bike Trip Count")
plt.title("Record Bike Trip Day of the Week")
plt.show()

# COMMAND ----------

display(historic_bike_trips.groupBy("day_of_the_week").count())

# COMMAND ----------

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col

historic_weather = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather').withColumn('human_timestamp', to_date(from_unixtime('dt', 'yyyy-MM-dd')))
display(historic_weather)


# COMMAND ----------

historic_weather.select("dt").count() > historic_weather.select("dt").dropDuplicates().count()

# COMMAND ----------

historic_bike_trips = historic_bike_trips.join(historic_weather, historic_bike_trips.start_date_column == historic_weather.human_timestamp,'inner')
display(historic_bike_trips)

# COMMAND ----------

historic_bike_trips.dropDuplicates(["human_timestamp"]).count()

# COMMAND ----------

daily_rides = historic_bike_trips.groupBy('start_date_column','day_of_the_week','main', 'description', 'icon') \
                                 .agg(count('ride_id').alias('num_rides'), avg("temp"), avg("feels_like"), avg("pressure"), avg("humidity"), avg("dew_point"),avg("uvi"), avg("clouds"), avg("visibility"), avg("wind_speed"), avg("wind_deg"), avg("pop"), avg("snow_1h"), avg("id"), avg("rain_1h")) \
                                 .orderBy('start_date_column')
display(daily_rides)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/bronze/'))

# COMMAND ----------

bike_station_info = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/G13/bronze/bike-station-info/").filter(col("name")==GROUP_STATION_ASSIGNMENT)
display(bike_station_info)

# COMMAND ----------

bike_trips = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/G13/bronze/bike-status/").filter(col("station_id")=="66db65aa-0aca-11e7-82f6-3863bb44ef7c")
display(bike_trips)

# COMMAND ----------

historic_bike_trips.schema

# COMMAND ----------

import holidays

# define NY state holidays
ny_holidays = holidays.US(state='NY', years=[2021, 2022, 2023])

# get federal holidays in US for the same years
us_holidays = holidays.US(years=[2021, 2022, 2023])

# combine both state and federal holidays into one list
combined_holidays = ny_holidays + us_holidays

# convert each date in the list to a string
combined_holidays = [d.strftime('%Y-%m-%d') for d in combined_holidays]
# print the combined list of holidays
for holiday in sorted(combined_holidays):
    print(holiday)
    


# COMMAND ----------

from pyspark.sql.functions import count, date_format, avg

# add new column indicating whether ride occurred on holiday
historic_bike_trips = historic_bike_trips.withColumn('is_holiday', col('start_date_column').isin(combined_holidays).cast('int'))

daily_rides = historic_bike_trips.groupBy('start_date_column','day_of_the_week', 'is_holiday') \
                                 .agg(count('ride_id').alias('num_rides')) \
                                 .orderBy('day_of_the_week', 'is_holiday')

# calculate average number of rides per day for each group
avg_daily_rides = daily_rides.groupBy('day_of_the_week', 'is_holiday') \
                             .agg((avg('num_rides'))) \
                             .orderBy('day_of_the_week', 'is_holiday')

display(avg_daily_rides)

# COMMAND ----------

daily_rides = historic_bike_trips.groupBy('start_date_column','day_of_the_week', 'is_holiday') \
                                 .agg(count('ride_id').alias('num_rides')) \
                                 .orderBy('day_of_the_week', 'is_holiday')
display(daily_rides)

# COMMAND ----------

#!pip install geospark

# COMMAND ----------

GeoSparkRegistrator.registerAll(spark)

# COMMAND ----------

from geospark.register import GeoSparkRegistrator
from pyspark.sql.functions import udf
from shapely.geometry import Point
from pyspark.sql.types import *


point_schema = StructType([StructField("latitude", DoubleType(), True), StructField("longitude", DoubleType(), True)])

@udf(point_schema)
def create_point(lat, lon):
    return [lat, lon]

# Convert historical_bike_trips to GeoDataFrame
historical_bike_trips_gdf = historic_bike_trips.filter(col("start_station_name")==GROUP_STATION_ASSIGNMENT) \
    .withColumn("geometry", create_point(col("start_lat"),col("start_lng"))) \
    .drop("start_lat", "start_lng")
    
# Convert weather data to GeoDataFrame
weather_data_gdf = historic_weather \
    .withColumn("geometry", create_point(col("lat"),col("lon"))) \
    .drop("lat", "lon")



# Spatial join bike trips GeoDataFrame and weather GeoDataFrame
joined_data = historical_bike_trips_gdf \
    .spatialJoin(weather_data_gdf, "contains") \
    .drop("geometry", "geometry_right")


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
