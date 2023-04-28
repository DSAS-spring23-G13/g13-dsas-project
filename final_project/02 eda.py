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

# MAGIC %md
# MAGIC ## Additional EDA

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G13/bronze"))

# COMMAND ----------

bike_station = f"dbfs:/FileStore/tables/G13/bronze/bike-station-info/"

# COMMAND ----------

bike_station_df = spark.read.format('delta').load(bike_station)
display(bike_station_df)

# COMMAND ----------

bike_station_df.head(1)

# COMMAND ----------

bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"

bike_status_df = spark.read.format('delta').load(bike_status)
display(bike_status_df)

# COMMAND ----------

bike_status_df.head(1)

# COMMAND ----------

# First, join the bike_trip_df with bike_station_df on start_station and station_id columns:
from pyspark.sql.functions import col

# bike_trip_with_station_info = daily_trips.join(bike_station_df, col("start_station") == col("station_id"), "left")

# COMMAND ----------

display(daily_trips_weather)

# COMMAND ----------

display(daily_trips)

# COMMAND ----------

historic_bike_trips_df.head(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional EDA Start

# COMMAND ----------

# MAGIC %md
# MAGIC What is the distribution of station capacities in the city? Are there any clusters or patterns?

# COMMAND ----------

import matplotlib.pyplot as plt

# Extract station capacities from the dataframe
station_capacities = bike_station_df.select('capacity').rdd.flatMap(lambda x: x).collect()

# Create a histogram of station capacities
plt.hist(station_capacities, bins=20)
plt.xlabel('Station Capacity')
plt.ylabel('Number of Stations')
plt.title('Distribution of Station Capacities')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Based on these results, we can see that there is a wide range of station capacities in the system, with the minimum capacity being 0, the maximum capacity being 123, and the average capacity being approximately 31.60. This information can be helpful in understanding the distribution of bikes across different stations and the potential demand for bikes at each station.

# COMMAND ----------

from pyspark.sql.functions import min, max, mean

capacity_stats = bike_station_df.select(
    min("capacity").alias("min_capacity"),
    max("capacity").alias("max_capacity"),
    mean("capacity").alias("avg_capacity"),
).collect()[0]

min_capacity = capacity_stats.min_capacity
max_capacity = capacity_stats.max_capacity
avg_capacity = capacity_stats.avg_capacity

print(f"Minimum station capacity: {min_capacity}")
print(f"Maximum station capacity: {max_capacity}")
print(f"Average station capacity: {avg_capacity:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC What is the distribution of the number of bikes available at stations?

# COMMAND ----------

from pyspark.sql.functions import count

num_bikes_stats = bike_status_df.groupBy("num_bikes_available").agg(count("*").alias("count")).orderBy("num_bikes_available")

# To output the result as a list of dictionaries
num_bikes_list = [row.asDict() for row in num_bikes_stats.collect()]

# print(num_bikes_list)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert num_bikes_list to a DataFrame
num_bikes_df = pd.DataFrame(num_bikes_list)

sns.set(style="whitegrid")
plt.figure(figsize=(16, 6))
sns.barplot(x="num_bikes_available", y="count", data=num_bikes_df)
plt.title("Number of Bikes Available at Stations")
plt.xlabel("Number of Bikes Available")
plt.ylabel("Number of Stations")
plt.xticks(rotation=90)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC What is the distribution of station status (active, inactive, etc.)?

# COMMAND ----------

station_status_stats = bike_status_df.groupBy("station_status").agg(count("*").alias("count")).orderBy("station_status")

# To output the result as a list of dictionaries
station_status_list = [row.asDict() for row in station_status_stats.collect()]

print(station_status_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Based on the output, we can observe that the majority of the stations are in an 'active' status (2,219,232), while a smaller number of stations are 'out_of_service' (40,854). This information is useful as it provides an understanding of the proportion of stations that are currently operational and can be used for forecasting net bike change.

# COMMAND ----------

# MAGIC %md
# MAGIC How do bike trips vary across different types of bikes (electric bikes, regular bikes, etc.)?

# COMMAND ----------

bike_type_stats = historic_bike_trips_df.groupBy("rideable_type").agg(count("*").alias("count")).orderBy("rideable_type")

# To output the result as a list of dictionaries
bike_type_list = [row.asDict() for row in bike_type_stats.collect()]

print(bike_type_list)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert bike_type_list to a DataFrame
bike_type_df = pd.DataFrame(bike_type_list)

sns.set(style="whitegrid")
plt.figure(figsize=(8, 6))
sns.barplot(x="rideable_type", y="count", data=bike_type_df)
plt.title("Bike Type Distribution")
plt.xlabel("Bike Type")
plt.ylabel("Number of Trips")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The output shows the distribution of bike trips across different types of bikes:
# MAGIC
# MAGIC Classic Bike: 29,585,988 trips  
# MAGIC Docked Bike: 306,185 trips  
# MAGIC Electric Bike: 10,506,882 trips   
# MAGIC
# MAGIC Classic bikes are the most frequently used, with a significantly higher number of trips compared to the other types. Electric bikes are the second most popular, while docked bikes have the least number of trips.
# MAGIC
# MAGIC Understanding the distribution of bike trips across different types of bikes can help us determine the influence of bike types on net bike change at the station level

# COMMAND ----------

# MAGIC %md
# MAGIC How do bike trips vary across different user types (members and casual users)?

# COMMAND ----------

user_type_stats = historic_bike_trips_df.groupBy("member_casual").agg(count("*").alias("count")).orderBy("member_casual")

# To output the result as a list of dictionaries
user_type_list = [row.asDict() for row in user_type_stats.collect()]

print(user_type_list)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Convert user_type_list to a DataFrame
user_type_df = pd.DataFrame(user_type_list)

sns.set(style="whitegrid")
plt.figure(figsize=(8, 6))
sns.barplot(x="member_casual", y="count", data=user_type_df)
plt.title("User Type Distribution")
plt.xlabel("User Type")
plt.ylabel("Number of Trips")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The output shows the distribution of bike trips across different user types:
# MAGIC  
# MAGIC Casual users: 8,262,716 trips  
# MAGIC Members: 32,136,339 trips  
# MAGIC Members account for a significantly larger number of trips compared to casual users.  
# MAGIC
# MAGIC Understanding the distribution of bike trips across different user types can help us determine the influence of user types on net bike change at the station level.

# COMMAND ----------

# MAGIC %md
# MAGIC What are the peak hours for bike usage, and how does this differ between weekdays and weekends?

# COMMAND ----------

from pyspark.sql.functions import hour, date_format

# Extract hour and day of week from the started_at column
historic_bike_trips_df = historic_bike_trips_df.withColumn("hour", hour("started_at"))
historic_bike_trips_df = historic_bike_trips_df.withColumn("day_of_week", date_format("started_at", "E"))

# Group by hour and day of the week, and count the number of trips
hour_day_stats = historic_bike_trips_df.groupBy("hour", "day_of_week").agg(count("*").alias("count")).orderBy("hour", "day_of_week")

# Collect the results as a list of dictionaries
hour_day_list = [row.asDict() for row in hour_day_stats.collect()]


# COMMAND ----------

# Convert hour_day_list to a DataFrame
hour_day_df = pd.DataFrame(hour_day_list)

# Create a heatmap to visualize bike usage by hour and day of the week
plt.figure(figsize=(20, 14))
sns.heatmap(hour_day_df.pivot("day_of_week", "hour", "count"), cmap="YlGnBu", annot=True, fmt="d")
plt.title("Bike Usage by Hour and Day of the Week")
plt.xlabel("Hour")
plt.ylabel("Day of the Week")
plt.show()

# COMMAND ----------

# Separate weekdays and weekends into different columns
hour_day_df["weekday_count"] = hour_day_df.apply(lambda row: row["count"] if row["day_of_week"] not in ["Sat", "Sun"] else 0, axis=1)
hour_day_df["weekend_count"] = hour_day_df.apply(lambda row: row["count"] if row["day_of_week"] in ["Sat", "Sun"] else 0, axis=1)

# Group by hour and sum the counts
hour_day_grouped = hour_day_df.groupby("hour").agg({"weekday_count": "sum", "weekend_count": "sum"}).reset_index()

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10, 5))
plt.plot(hour_day_grouped["hour"], hour_day_grouped["weekday_count"], label="Weekdays", marker="o")
plt.plot(hour_day_grouped["hour"], hour_day_grouped["weekend_count"], label="Weekends", marker="o")
plt.title("Bike Usage by Hour (Weekdays vs Weekends)")
plt.xlabel("Hour")
plt.ylabel("Number of Trips")
plt.xticks(range(0, 24))
plt.legend()
plt.grid()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Are there any correlations between weather conditions and bike availability at different stations?

# COMMAND ----------

# MAGIC %md
# MAGIC Merge the nyc_weather_df with the bike_status_df based on timestamps. Since nyc_weather_df has a column named dt representing timestamps, we will use it to join the dataframes. You may need to round or truncate timestamps to the nearest hour or day, depending on the granularity of your weather data.

# COMMAND ----------

# from pyspark.sql.functions import from_unixtime, date_trunc

# # Convert the 'dt' column to a timestamp and round it to the nearest hour
# nyc_weather_df = nyc_weather_df.withColumn("timestamp", date_trunc("hour", from_unixtime("dt")))

# # Merge bike_status_df with nyc_weather_df based on timestamps
# merged_df = bike_status_df.join(nyc_weather_df, bike_status_df["timestamp"] == nyc_weather_df["timestamp"], "inner")

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, date_trunc
from pyspark.sql.types import TimestampType

# Convert the 'last_reported' column to a timestamp and round it to the nearest hour
bike_status_df = bike_status_df.withColumn("rounded_last_reported", date_trunc("hour", from_unixtime("last_reported").cast(TimestampType())))

# Merge bike_status_df with nyc_weather_df based on rounded_last_reported and timestamp columns
merged_df = bike_status_df.join(nyc_weather_df, bike_status_df["rounded_last_reported"] == nyc_weather_df["timestamp"], "inner")

# COMMAND ----------

# Calculate the average number of available bikes for each unique weather condition.
from pyspark.sql.functions import avg

weather_bike_availability = merged_df.groupBy("temp", "clouds", "humidity", "wind_speed")\
                                    .agg(avg("num_bikes_available").alias("avg_num_bikes_available"))

# COMMAND ----------

# Compute the correlation between weather features (temperature, clouds, humidity, and wind speed) and bike availability using the Pearson correlation coefficient.
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler

# Assemble the weather features into a single column
assembler = VectorAssembler(inputCols=["temp", "clouds", "humidity", "wind_speed"], outputCol="features")
weather_bike_availability_vec = assembler.transform(weather_bike_availability)

# Compute the correlation matrix
correlation_matrix = Correlation.corr(weather_bike_availability_vec, "features").collect()[0][0]

# COMMAND ----------

# Convert the correlation matrix to a dictionary
correlation_dict = {}
columns = ["temp", "clouds", "humidity", "wind_speed"]

for i in range(len(columns)):
    for j in range(i + 1, len(columns)):
        key = f"{columns[i]}_{columns[j]}"
        value = correlation_matrix[i, j]
        correlation_dict[key] = value

# Print the correlation dictionary
print(correlation_dict)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

# Convert the correlation matrix to a NumPy array
correlation_array = np.array(correlation_matrix.toArray())

# Create a heatmap using matplotlib
fig, ax = plt.subplots()
cax = ax.matshow(correlation_array, cmap='coolwarm', vmin=-1, vmax=1)
fig.colorbar(cax)

# Set the axis labels
ax.set_xticks(range(len(columns)))
ax.set_yticks(range(len(columns)))
ax.set_xticklabels(columns)
ax.set_yticklabels(columns)

# Rotate the x-axis labels
plt.xticks(rotation=45)

# Add title
plt.title("Correlation Matrix Heatmap")

# Display the heatmap
plt.show()

# COMMAND ----------

# from pyspark.sql.functions import col
# merged_df = merged_df.withColumn("bike_availability_ratio", col("num_bikes_available") / (col("num_bikes_available") + col("num_docks_available")))

from pyspark.sql.functions import col

# Filter out rows with both num_bikes_available and num_docks_available equal to zero
filtered_df = merged_df.filter((col("num_bikes_available") > 0) | (col("num_docks_available") > 0))

# Calculate the bike_availability_ratio
merged_df = filtered_df.withColumn("bike_availability_ratio", col("num_bikes_available") / (col("num_bikes_available") + col("num_docks_available")))

# COMMAND ----------

merged_df.head(1)

# COMMAND ----------

from pyspark.sql.functions import avg
weather_availability_df = merged_df.groupBy("temp", "clouds", "humidity", "wind_speed").agg(avg("bike_availability_ratio").alias("average_availability_ratio"))

# COMMAND ----------

import matplotlib.pyplot as plt

temp_ratio_data = weather_availability_df.select("temp", "average_availability_ratio").collect()
temp = [row["temp"] for row in temp_ratio_data]
availability_ratio = [row["average_availability_ratio"] for row in temp_ratio_data]

plt.scatter(temp, availability_ratio)
plt.xlabel("Temperature")
plt.ylabel("Average Bike Availability Ratio")
plt.title("Temperature vs. Bike Availability Ratio")
plt.show()

# COMMAND ----------

print("Clouds:", clouds[:10])
print("Humidity:", humidity[:10])
print("Wind Speed:", wind_speed[:10])
print("Average Bike Availability Ratio:", availability_ratio[:10])

# COMMAND ----------

import matplotlib.pyplot as plt

# Remove None values from availability_ratio
availability_ratio = [value for value in availability_ratio if value is not None]

# Create subplots
fig, axes = plt.subplots(1, 3, figsize=(18, 6))

# Clouds vs. Bike Availability
axes[0].scatter(clouds, availability_ratio, alpha=0.5)
axes[0].set_title('Clouds vs. Bike Availability Ratio')
axes[0].set_xlabel('Clouds')
axes[0].set_ylabel('Bike Availability Ratio')

# Humidity vs. Bike Availability
axes[1].scatter(humidity, availability_ratio, alpha=0.5)
axes[1].set_title('Humidity vs. Bike Availability Ratio')
axes[1].set_xlabel('Humidity')
axes[1].set_ylabel('Bike Availability Ratio')

# Wind Speed vs. Bike Availability
axes[2].scatter(wind_speed, availability_ratio, alpha=0.5)
axes[2].set_title('Wind Speed vs. Bike Availability Ratio')
axes[2].set_xlabel('Wind Speed')
axes[2].set_ylabel('Bike Availability Ratio')

# Show the plots
plt.show()


# COMMAND ----------


