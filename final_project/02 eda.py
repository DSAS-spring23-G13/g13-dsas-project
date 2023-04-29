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

NYC_WEATHER_FILE_PATH
BIKE_TRIP_DATA_PATH
BRONZE_STATION_INFO_PATH
BRONZE_STATION_STATUS_PATH
BRONZE_NYC_WEATHER_PATH

old_weather_df = spark.read.format("csv").option("header", "true").load(NYC_WEATHER_FILE_PATH)
display(old_weather_df)

# COMMAND ----------

old_weather_df.printSchema()

# COMMAND ----------

old_bike_info_fs = [file.path for file in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if file.name.endswith("_citibike_tripdata.csv")]

old_bike_info_fs

# COMMAND ----------

# lets start with last years trend
oldb_df = spark.read.format("csv").option("header", "true").load('dbfs:/FileStore/tables/raw/bike_trips/202301_citibike_tripdata.csv')
display(oldb_df)

# COMMAND ----------

# unique rideable
unique_values = [row.rideable_type for row in oldb_df.select("rideable_type").distinct().collect()]
# unique_values # ['docked_bike', 'classic_bike', 'electric_bike']

# how about some commonly start start_station_name

# COMMAND ----------

dbutils.fs.ls(BIKE_TRIP_DATA_PATH)

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13'))
#display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/historic_weather'))

# COMMAND ----------

from pyspark.sql.functions import *
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
display(trip_info)

# COMMAND ----------

weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
display(weather_info)   

# COMMAND ----------

## Data summary for the historic data
display(weather_info.describe())

# COMMAND ----------

# Load and filter the data
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))
display(weather_data)

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
station_trips = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))


# Aggregate the trips by year and month
monthly_trips = station_trips.withColumn("year", year("started_at")).withColumn("month", month("started_at")).groupBy("year", "month").agg(count("*").alias("trip_count")).orderBy("year","month")

# Convert the Spark DataFrame to a pandas DataFrame
monthly_trips_pd = monthly_trips.toPandas()

# Plot the results using a line chart
plt.figure(figsize=(15, 6))
plt.bar(monthly_trips_pd["year"].astype(str) + "-" + monthly_trips_pd["month"].astype(str).str.zfill(2), monthly_trips_pd["trip_count"])
plt.title("Monthly trips trends for Lafayette St & E 8 St")
plt.xlabel("Month")
plt.ylabel("Trip count")
plt.show()


# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
station_trips = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Aggregate the trips by date
daily_trips = station_trips.groupBy("date").agg(count("*").alias("trip_count")).orderBy("date")

# Convert the Spark DataFrame to a pandas DataFrame
daily_trips_pd = daily_trips.toPandas()

# Plot the results using a line chart
plt.figure(figsize=(20, 6))
plt.plot(daily_trips_pd["date"], daily_trips_pd["trip_count"])
plt.title("Daily trip trends for Lafayette St & E 8 St")
plt.xlabel("Date")
plt.ylabel("Trip count")
plt.show()

# COMMAND ----------

from pyspark.sql.functions import *
import matplotlib.pyplot as plt

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
station_trips = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Extract the day of the week from the date column
station_trips = station_trips.withColumn("day_of_week", date_format(col("date"), "E"))

# Aggregate the trips by day of the week
daily_trips = station_trips.groupBy("day_of_week").agg(count("*").alias("trip_count")).orderBy("day_of_week")

# Convert the Spark DataFrame to a pandas DataFrame
daily_trips_pd = daily_trips.toPandas()

# Plot the results using a line chart
plt.figure(figsize=(20, 6))
plt.bar(daily_trips_pd["day_of_week"], daily_trips_pd["trip_count"])
plt.title("Daily trip trends for Lafayette St & E 8 St")
plt.xlabel("Day of the week")
plt.ylabel("Trip count")
plt.show()

# COMMAND ----------

import holidays
import matplotlib.pyplot as plt

# Create a list of holidays for the US in 2022
us_holidays = holidays.US(years=2022)

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
station_trips = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Filter the holidays that fall within the date range of the station_trips dataframe
holiday_dates = [date for date in us_holidays.keys() if date >= station_trips.selectExpr("min(date)").collect()[0][0] and date <= station_trips.selectExpr("max(date)").collect()[0][0]]

# Convert the list of holiday dates to a Spark dataframe
holidays_df = spark.createDataFrame([(date,) for date in holiday_dates], ["date"])

# Join the two dataframes on the date column
trips_with_holidays = station_trips.join(holidays_df, "date", "left")

# Count the number of trips on each holiday
holiday_trips = trips_with_holidays.groupBy("date").agg(count("*").alias("trips"))

# Convert the Spark dataframe to a Pandas dataframe
pandas_df = holiday_trips.toPandas()

# Set the index of the Pandas dataframe to the holiday date
pandas_df.set_index("date", inplace=True)

# Plot the number of trips on each holiday
plt.figure(figsize=(10,6))
plt.bar(pandas_df.index, pandas_df["trips"])
plt.xticks(rotation=45)
plt.ylabel("Number of trips")
plt.title("Number of trips on holidays")
plt.show()

# COMMAND ----------

### Creating function to take into account weekends
from datetime import datetime, timedelta

# Function to generate weekend dates
def generate_weekend_dates(start_date, end_date):
    weekends = []
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    while start_date <= end_date:
        if start_date.weekday() >= 5:  # Saturday and Sunday have weekdays 5 and 6
            weekends.append(str(start_date.date()))
        start_date += timedelta(days=1)
    
    return weekends

# Generate weekend dates for the dataset's date range
weekends = generate_weekend_dates("2021-01-01", "2023-03-31")

# COMMAND ----------

## Check the effect of holidays on bike trips

import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import *

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
station_trips = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Aggregate the trips by day of the week
daily_trips = station_trips.groupBy("date").agg(count("*").alias("trip_count")).orderBy("date") 
holidays = [
    "2021-01-01",  # New Year's Day
    "2021-01-18",  # Martin Luther King Jr. Day
    "2021-02-15",  # Presidents' Day
    "2021-05-31",  # Memorial Day
    "2021-07-04",  # Independence Day
    "2021-09-06",  # Labor Day
    "2021-10-11",  # Columbus Day
    "2021-11-11",  # Veterans Day
    "2021-11-25",  # Thanksgiving Day
    "2021-12-25",  # Christmas Day
    "2022-01-01",  # New Year's Day
    "2022-01-17",  # Martin Luther King Jr. Day
    "2022-02-21",  # Presidents' Day
    "2022-05-30",  # Memorial Day
    "2022-07-04",  # Independence Day
    "2022-09-05",  # Labor Day
    "2022-10-10",  # Columbus Day
    "2022-11-11",  # Veterans Day
    "2022-11-24",  # Thanksgiving Day
    "2022-12-25",  # Christmas Day
    "2023-01-01",  # New Year's Day
    "2023-01-16",  # Martin Luther King Jr. Day
    "2023-02-20",  # Presidents' Day
    "2023-05-29",  # Memorial Day
    "2023-07-04",  # Independence Day
    "2023-09-04",  # Labor Day
    "2023-10-09",  # Columbus Day
    "2023-11-11",  # Veterans Day
    "2023-11-23",  # Thanksgiving Day
    "2023-12-25",  # Christmas Day
] + weekends

# Convert holiday dates to a set for faster lookups
holiday_set = set(holidays)


# Create a user-defined function (UDF) to check if a date is a holiday
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

@udf(returnType=BooleanType())
def is_holiday(date):
    return str(date) in holiday_set



# Add a new column to the daily_trips DataFrame to indicate if the date is a holiday or not
daily_trips = daily_trips.withColumn("is_holiday", is_holiday("date"))


# Calculate the average trip counts for holidays and non-holidays
avg_trips = daily_trips.groupBy("is_holiday").agg(avg("trip_count").alias("average_trip_count")
)

avg_trips.show()

avg_trips_pd = avg_trips.toPandas()

# Set the plot size
plt.figure(figsize=(10, 6))

# Create a bar plot
plt.bar(avg_trips_pd['is_holiday'], avg_trips_pd['average_trip_count'], color=['blue', 'green'])

# Set the plot title, x-label, and y-label
plt.title("Average Daily Trips by Holiday Status")
plt.xlabel("Is Holiday?")
plt.ylabel("Average Trip Count")

# Customize the x-axis tick labels
plt.xticks([0, 1], ['Non-Holiday', 'Holiday'])

# Display the plot
plt.show()

# COMMAND ----------

## Merge the bike info and weather data
from pyspark.sql.functions import *
joined_trip= trip_info.join(weather_data, trip_info['date']== weather_data["date_weather"], "inner" )
joined_trip = joined_trip.drop("date_weather")
display(joined_trip)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
