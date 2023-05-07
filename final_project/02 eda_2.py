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

# This code will print the most frequent start and end stations for trips that are to or from the given station.
from pyspark.sql.functions import desc

GROUP_STATION_ASSIGNMENT = "Franklin Ave & St Marks Ave"  # Replace with your assigned station

# Filter trips starting at the given station
start_station_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT)

# Group by end_station_name and count the number of trips
start_station_grouped = start_station_df.groupBy("end_station_name").agg(F.count("ride_id").alias("trip_count"))

# Sort in descending order based on trip_count and take the first row
most_frequent_end_station = start_station_grouped.sort(desc("trip_count")).first()

# Filter trips ending at the given station
end_station_df = historic_bike_trips_df.filter(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT)

# Group by start_station_name and count the number of trips
end_station_grouped = end_station_df.groupBy("start_station_name").agg(F.count("ride_id").alias("trip_count"))

# Sort in descending order based on trip_count and take the first row
most_frequent_start_station = end_station_grouped.sort(desc("trip_count")).first()

print(f"Most frequent start station: {most_frequent_start_station['start_station_name']} with {most_frequent_start_station['trip_count']} trips")
print(f"Most frequent end station: {most_frequent_end_station['end_station_name']} with {most_frequent_end_station['trip_count']} trips")

# COMMAND ----------

# bar chart showing the trip count for the top N stations
import matplotlib.pyplot as plt

# Number of top stations to display
top_n = 10

# Get the top N start stations
top_start_stations = end_station_grouped.sort(desc("trip_count")).take(top_n)

# Get the top N end stations
top_end_stations = start_station_grouped.sort(desc("trip_count")).take(top_n)

# Extract the station names and trip counts for start stations
start_station_names = [row['start_station_name'] for row in top_start_stations]
start_station_trip_counts = [row['trip_count'] for row in top_start_stations]

# Extract the station names and trip counts for end stations
end_station_names = [row['end_station_name'] for row in top_end_stations]
end_station_trip_counts = [row['trip_count'] for row in top_end_stations]

# Create a bar chart for the top N start stations
plt.figure(figsize=(12, 6))
plt.bar(start_station_names, start_station_trip_counts)
plt.xlabel('Start Stations')
plt.ylabel('Trip Count')
plt.title(f'Top {top_n} Start Stations for Trips from {GROUP_STATION_ASSIGNMENT}')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# Create a bar chart for the top N end stations
plt.figure(figsize=(12, 6))
plt.bar(end_station_names, end_station_trip_counts)
plt.xlabel('End Stations')
plt.ylabel('Trip Count')
plt.title(f'Top {top_n} End Stations for Trips to {GROUP_STATION_ASSIGNMENT}')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Knowing the most frequent start and end stations for trips involving your assigned station can help in making informed decisions related to bike-sharing operations, marketing strategies, and infrastructure planning. Here are some possible actions you can take based on this information:
# MAGIC
# MAGIC Optimize bike availability: Ensure that there are enough bikes available at the most frequent start station (Lefferts Pl & Franklin Ave) and enough empty docks at the most frequent end station (Eastern Pkwy & Franklin Ave) during peak hours. This can be done by redistributing bikes or adjusting the dock capacities.
# MAGIC
# MAGIC Infrastructure improvements: If the demand for bikes at these popular stations is consistently high, consider expanding the existing stations or adding new stations nearby to cater to the demand and reduce congestion.
# MAGIC
# MAGIC Targeted marketing: Promote membership plans or special offers to users who frequently start or end their trips at these popular stations. This can help in attracting new customers and retaining existing ones.
# MAGIC
# MAGIC Collaborate with local businesses: Partner with businesses near the most frequent start and end stations to offer promotions, discounts, or events that encourage more people to use the bike-sharing service.
# MAGIC
# MAGIC Analyze trip patterns: Further analyze the trip data to identify trends or patterns related to these popular stations, such as the time of day when they are most frequently used or if there are any seasonal variations. This information can be used to fine-tune operations, marketing strategies, and customer service.

# COMMAND ----------

from pyspark.sql.functions import hour, month

# Filter the data to include only trips starting or ending at the most frequent stations
frequent_stations_df = historic_bike_trips_df.filter(
    (historic_bike_trips_df.start_station_name == "Lefferts Pl & Franklin Ave") |
    (historic_bike_trips_df.end_station_name == "Eastern Pkwy & Franklin Ave")
)

# Extract the hour and month from the 'started_at' column
frequent_stations_df = frequent_stations_df.withColumn("hour", hour("started_at")).withColumn("month", month("started_at"))

# Group by hour and month, and count the trips
hourly_monthly_trips = frequent_stations_df.groupBy("hour", "month").agg(
    F.count("ride_id").alias("trip_count")
)

# Order the result by hour and month
hourly_monthly_trips = hourly_monthly_trips.orderBy("hour", "month")

# Display the result
hourly_monthly_trips.show()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Convert the hourly_monthly_trips DataFrame to a Pandas DataFrame
hourly_monthly_trips_pd = hourly_monthly_trips.toPandas()

# Pivot the data to create a matrix with hours as rows, months as columns, and trip_count as values
heatmap_data = hourly_monthly_trips_pd.pivot("hour", "month", "trip_count")

# Create the heatmap
plt.figure(figsize=(12, 6))
sns.heatmap(heatmap_data, annot=True, fmt=".0f", cmap="YlGnBu", cbar_kws={'label': 'Trip Count'})
plt.title("Hourly-Monthly Trips for Most Frequent Start and End Stations")
plt.xlabel("Month")
plt.ylabel("Hour")
plt.show()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Convert the hourly_monthly_trips DataFrame to a Pandas DataFrame
hourly_monthly_trips_pd = hourly_monthly_trips.toPandas()

# Create a FacetGrid with a separate line plot for each month
g = sns.FacetGrid(hourly_monthly_trips_pd, col="month", col_wrap=4, height=3, aspect=1.5)
g.map(sns.lineplot, "hour", "trip_count", marker="o")

# Customize the plot titles and labels
g.set_axis_labels("Hour", "Trip Count")
g.set_titles("Month {col_name}")
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle("Hourly Trips for Most Frequent Start and End Stations by Month", fontsize=16)

# Display the plots
plt.show()

# COMMAND ----------

from pyspark.sql import functions as F

# Calculate the most frequent start and end stations
start_stations_sorted = historic_bike_trips_df.groupBy("start_station_name").count().sort(F.desc("count"))
end_stations_sorted = historic_bike_trips_df.groupBy("end_station_name").count().sort(F.desc("count"))

# Extract the most frequent start and end station names
most_frequent_start_station = start_stations_sorted.first()["start_station_name"]
most_frequent_end_station = end_stations_sorted.first()["end_station_name"]

# Filter trips for the most frequent start and end stations
start_station_trips = historic_bike_trips_df.filter(F.col("start_station_name") == most_frequent_start_station)
end_station_trips = historic_bike_trips_df.filter(F.col("end_station_name") == most_frequent_end_station)

# Group by hour and count the trips for start station
start_station_hourly_trips = start_station_trips.groupBy(F.hour("started_at").alias("hour")).agg(F.count("ride_id").alias("trip_count")).orderBy("hour")

# Group by hour and count the trips for end station
end_station_hourly_trips = end_station_trips.groupBy(F.hour("ended_at").alias("hour")).agg(F.count("ride_id").alias("trip_count")).orderBy("hour")

# Convert to Pandas DataFrames
start_station_hourly_trips_pd = start_station_hourly_trips.toPandas()
end_station_hourly_trips_pd = end_station_hourly_trips.toPandas()

# Plot the line graphs for both stations
import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))
plt.plot(start_station_hourly_trips_pd["hour"], start_station_hourly_trips_pd["trip_count"], label=f"{most_frequent_start_station} (Start)")
plt.plot(end_station_hourly_trips_pd["hour"], end_station_hourly_trips_pd["trip_count"], label=f"{most_frequent_end_station} (End)")
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Trips")
plt.title("Hourly Trips for Most Frequent Start and End Stations")
plt.legend()
plt.xticks(range(0, 24))
plt.grid()
plt.show()

# COMMAND ----------

# Join the start_station_hourly_trips and end_station_hourly_trips DataFrames
hourly_trips_both_stations = start_station_hourly_trips.alias("start").join(
    end_station_hourly_trips.alias("end"), F.col("start.hour") == F.col("end.hour"), "inner"
).select(
    F.col("start.hour").alias("hour"),
    F.col("start.trip_count").alias("start_station_trip_count"),
    F.col("end.trip_count").alias("end_station_trip_count")
).orderBy("hour")

# Convert the hourly_trips_both_stations DataFrame to a Pandas DataFrame
hourly_trips_both_stations_pd = hourly_trips_both_stations.toPandas()

# Create a FacetGrid with a separate line plot for each station
g = sns.FacetGrid(hourly_trips_both_stations_pd, height=4, aspect=2)
g.map(sns.lineplot, "hour", "start_station_trip_count", marker="o", label=f"{most_frequent_start_station} (Start)")
g.map(sns.lineplot, "hour", "end_station_trip_count", marker="o", label=f"{most_frequent_end_station} (End)")

# Customize the plot titles and labels
g.set_axis_labels("Hour", "Trip Count")
g.add_legend(title="Station")
g.fig.subplots_adjust(top=0.9)
g.fig.suptitle("Hourly Trips for Most Frequent Start and End Stations", fontsize=16)

# Display the plots
plt.show()
