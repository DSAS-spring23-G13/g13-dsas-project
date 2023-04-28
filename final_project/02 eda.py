# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G13/"))

# COMMAND ----------

# display(dbutils.fs.ls(f"{GROUP_DATA_PATH}historic_bike_trips"))
HISTORIC_BIKE_TRIPS = f"dbfs:/FileStore/tables/G13/historic_bike_trips/"

historic_bike_trips_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)
display(historic_bike_trips_df)

# COMMAND ----------

historic_bike_trips_df.columns

# COMMAND ----------

from pyspark.sql import functions as F

# Extract the year and month from the 'started_at' column
# lafayette_df = historic_bike_trips_df.withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))
# Extract the year and month from the 'started_at' column
lafayette_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))

# Group by year and month, and count the trips
monthly_trips = lafayette_df.groupBy("year", "month").agg(
    F.count("ride_id").alias("trip_count")
)

# Order the result by year and month
monthly_trips = monthly_trips.orderBy("year", "month")

# Display the result
monthly_trips.show()

# COMMAND ----------



# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convert the Spark DataFrame to a Pandas DataFrame
monthly_trips_pd = monthly_trips.toPandas()

# Create a new column with a string representation of year and month
monthly_trips_pd['year_month'] = monthly_trips_pd['year'].astype(str) + '-' + monthly_trips_pd['month'].astype(str).str.zfill(2)

# Set the figure size and title
plt.figure(figsize=(15, 6))
plt.title("Monthly Trip Trends for Lafayette Station")

# Create a bar chart
plt.bar(monthly_trips_pd['year_month'], monthly_trips_pd['trip_count'])

# Set the x and y axis labels
plt.xlabel("Year-Month")
plt.ylabel("Trip Count")

# Rotate the x-axis labels for better readability
plt.xticks(rotation=45)

# Display the chart in Databricks
display(plt.show())

# COMMAND ----------

from pyspark.sql import functions as F

# Extract the date from the 'started_at' column
lafayette_df = historic_bike_trips_df.withColumn("date", F.to_date("started_at"))

# Group by date and count the trips
daily_trips = lafayette_df.groupBy("date").agg(
    F.count("ride_id").alias("trip_count")
)

# Order the result by date
daily_trips = daily_trips.orderBy("date")

# Display the result
daily_trips.show()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convert the Spark DataFrame to a Pandas DataFrame
daily_trips_pd = daily_trips.toPandas()

# Set the figure size and title
plt.figure(figsize=(15, 6))
plt.title("Daily Trip Trends for Lafayette Station")

# Create a line chart
plt.plot(daily_trips_pd['date'], daily_trips_pd['trip_count'])

# Set the x and y axis labels
plt.xlabel("Date")
plt.ylabel("Trip Count")

# Rotate the x-axis labels for better readability
plt.xticks(rotation=45)

# Display the chart in Databricks
display(plt.show())

# COMMAND ----------

# This code snippet includes major federal holidays observed in Lafayette, New York, and weekends for the specified date range. Remember to adjust the date range in the generate_weekend_dates() function if needed.

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

# COMMAND ----------



# COMMAND ----------

# Create a user-defined function (UDF) to check if a date is a holiday
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

@udf(returnType=BooleanType())
def is_holiday(date):
    return str(date) in holiday_set

# COMMAND ----------

# Add a new column to the daily_trips DataFrame to indicate if the date is a holiday or not
daily_trips = daily_trips.withColumn("is_holiday", is_holiday("date"))

# COMMAND ----------

# Calculate the average trip counts for holidays and non-holidays
avg_trips = daily_trips.groupBy("is_holiday").agg(
    F.avg("trip_count").alias("average_trip_count")
)

avg_trips.show()

# COMMAND ----------

import pandas as pd

avg_trips_pd = avg_trips.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt

# Set the plot size
plt.figure(figsize=(10, 6))

# Create a bar plot
plt.bar(avg_trips_pd['is_holiday'], avg_trips_pd['average_trip_count'], color=['blue', 'orange'])

# Set the plot title, x-label, and y-label
plt.title("Average Daily Trips by Holiday Status")
plt.xlabel("Is Holiday?")
plt.ylabel("Average Trip Count")

# Customize the x-axis tick labels
plt.xticks([0, 1], ['Non-Holiday', 'Holiday'])

# Display the plot
plt.show()

# COMMAND ----------

# Read the weather data
nyc_weather_df = spark.read.csv(
    "dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

nyc_weather_df.head(5)

# COMMAND ----------

# Convert the timestamp in the weather data to the appropriate granularity (daily or hourly). For this we'll use daily granularity
from pyspark.sql.functions import to_date, from_unixtime

# Convert the 'dt' column from Unix timestamp to a date type column
nyc_weather_df = nyc_weather_df.withColumn("date", to_date(from_unixtime("dt")))

# Show the content and schema of the nyc_weather_df DataFrame
nyc_weather_df.head(2)

# COMMAND ----------

# Aggregate weather data by date
weather_daily_agg = nyc_weather_df.groupBy("date").agg(
    F.avg("temp").alias("avg_temperature"),
    F.avg("rain_1h").alias("avg_precipitation"),
    F.avg("wind_speed").alias("avg_wind_speed")
)

# Show the content and schema of the weather_daily_agg DataFrame
weather_daily_agg.show()
weather_daily_agg.printSchema()

# COMMAND ----------

# Join the aggregated weather data with the daily_trips DataFrame on the date (or hour) column:
daily_trips_weather = daily_trips.join(weather_daily_agg, on="date")

# COMMAND ----------

# Analyze the correlation between the daily/hourly trip counts and weather variables using the corr() function:
temp_corr = daily_trips_weather.stat.corr("trip_count", "avg_temperature")
precip_corr = daily_trips_weather.stat.corr("trip_count", "avg_precipitation")
wind_speed_corr = daily_trips_weather.stat.corr("trip_count", "avg_wind_speed")

print(f"Correlation between trip_count and avg_temperature: {temp_corr}")
print(f"Correlation between trip_count and avg_precipitation: {precip_corr}")
print(f"Correlation between trip_count and avg_wind_speed: {wind_speed_corr}")

# COMMAND ----------

# Visualize the relationships between trip counts and weather variables using scatter plots. First, convert the daily_trips_weather DataFrame to a Pandas DataFrame:
daily_trips_weather_pd = daily_trips_weather.toPandas()

# COMMAND ----------

import matplotlib.pyplot as plt

# Create a 3x1 subplot grid
fig, ax = plt.subplots(3, 1, figsize=(12, 18))

# Create scatter plots for each weather variable
ax[0].scatter(daily_trips_weather_pd['avg_temperature'], daily_trips_weather_pd['trip_count'], alpha=0.5)
ax[0].set_title("Daily Trips vs. Average Temperature")
ax[0].set_xlabel("Average Temperature")
ax[0].set_ylabel("Daily Trips")

ax[1].scatter(daily_trips_weather_pd['avg_precipitation'], daily_trips_weather_pd['trip_count'], alpha=0.5)
ax[1].set_title("Daily Trips vs. Average Precipitation")
ax[1].set_xlabel("Average Precipitation")
ax[1].set_ylabel("Daily Trips")

ax[2].scatter(daily_trips_weather_pd['avg_wind_speed'], daily_trips_weather_pd['trip_count'], alpha=0.5)
ax[2].set_title("Daily Trips vs. Average Wind Speed")
ax[2].set_xlabel("Average Wind Speed")
ax[2].set_ylabel("Daily Trips")

# Display the plots
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Based on the correlation values, we can make the following observations:
# MAGIC
# MAGIC There's a strong positive correlation between the trip count and average temperature (0.76). This suggests that as the temperature increases, the number of trips taken also tends to increase. People may be more likely to use the bike-sharing system when the weather is warmer.
# MAGIC
# MAGIC There's a weak negative correlation between the trip count and average precipitation (-0.21). This indicates that as precipitation increases, the number of trips taken may decrease slightly. Rain or snow could make it less desirable for people to use bikes.
# MAGIC
# MAGIC There's a weak negative correlation between the trip count and average wind speed (-0.14). This suggests that as the wind speed increases, the number of trips taken may decrease a little. High winds might make biking less enjoyable or more difficult.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
