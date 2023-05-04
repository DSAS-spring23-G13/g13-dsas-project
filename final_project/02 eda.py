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

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC
# MAGIC -- drop table if exists bike_station_info;?
# MAGIC show tables;
# MAGIC -- select * from bike_station_info limit 10;

# COMMAND ----------

display(test_df_1.groupBy(col("rideable_type")).count())

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13'))
# display(dbutils.fs.rm('dbfs:/FileStore/tables/G13/historic_weather', recurse = True))

# COMMAND ----------

## Load the data based on every trip to and fro our station ie. Lafayette St & E 8 St
from pyspark.sql.functions import *
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(
    (col("start_station_name")== "Lafayette t & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))

trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", ((unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60).cast("double")) ## ours in mins

display(trip_info)

# COMMAND ----------

# MAGIC %md
# MAGIC i. What are the monthly trip trends for your assigned station?

# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px

trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(
    (col("start_station_name") == "Lafayette St & E 8 St") | (col("end_station_name") == "Lafayette St & E 8 St"))

trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", ((unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 60).cast("double")) ## hours in mins
trip_info = trip_info.withColumn("timestamp", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss"))

monthly_trips = trip_info.groupBy(month(col("date")).alias("month")).agg(
    count(when(col("start_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_starting_here"),
    count(when(col("end_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_ending_here")
).orderBy("month")

display(monthly_trips)

fig = px.line(monthly_trips.toPandas(), x='month', y=['trips_starting_here', 'trips_ending_here'],
              labels={'month':'Month', 'value':'Frequency of trips', 'variable':'Key'},
              title='Monthly Trip Trends for Lafayette St & E 8 St Station')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Comment:

# COMMAND ----------

# MAGIC %md
# MAGIC ii. What are the daily trip trends for your given station?

# COMMAND ----------

# Show which hour of the day has the highest trip count
from pyspark.sql.functions import hour

# Read the trip info data
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips')

# Calculate the duration of each trip in minutes
trip_info = trip_info.withColumn("duration_minutes", (col("ended_at").cast("long") - col("started_at").cast("long")) / 60)

# Group the trips by hour and count the number of trips for each hour
hourly_trips = trip_info.groupBy(hour("started_at").alias("hour")).count()

# Show the hourly trips data
display(hourly_trips)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Comment:
# MAGIC From the above graph, it can be observed bike trips are mostly high at 5pm(17th hr) each day.

# COMMAND ----------

weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
display(weather_info)   

# COMMAND ----------




# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go

# read the historic bike trips data from delta lake and filter for the assigned station
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(
    (col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))

# extract date and calculate trip duration in hours
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", ((unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 3600).cast("double")) ## hours in decimal format
trip_info = trip_info.withColumn("timestamp", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss"))

# group by day and calculate daily trip counts
daily_trips = trip_info.groupBy(col("date")).agg(
    count(col("ride_id")).alias("trips_starting_here"),
    count(col("ride_id")).alias("trips_ending_here")
).orderBy(col("date"))

# plot daily trip counts
fig = go.Figure()
fig = px.line(daily_trips.toPandas(), x="date", y=["trips_starting_here", "trips_ending_here"], 
              labels={"date": "Date", "value": "Number of Trips", "variable": "Trips"}, 
              title="Daily Trip Trends for Lafayette St & E 8 St")
fig.update_layout(width=1700, height=500)
fig.show()



# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go

# read the historic bike trips data from delta lake and filter for the assigned station
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(
    (col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))

# extract date and calculate trip duration in hours
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", ((unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))) / 3600).cast("double")) ## hours in decimal format
trip_info = trip_info.withColumn("timestamp", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss"))

# group by day and calculate daily trip counts
daily_trips = trip_info.groupBy(col("date")).agg(
    count(col("ride_id")).alias("trips_starting_here"),
    count(col("ride_id")).alias("trips_ending_here")
).orderBy(col("date"))

# group by day of month and calculate average daily trip counts
monthly_day_trips = daily_trips.groupBy(dayofmonth(col("date")).alias("day")).agg(
    avg(col("trips_starting_here")).alias("avg_trips_starting_here"),
    avg(col("trips_ending_here")).alias("avg_trips_ending_here")
).orderBy(col("day"))

# plot average daily trip counts by day of month
fig = go.Figure()
fig.add_trace(go.Scatter(x=monthly_day_trips.select(col("day")).rdd.flatMap(lambda x: x).collect(),
                         y=monthly_day_trips.select(col("avg_trips_starting_here")).rdd.flatMap(lambda x: x).collect(),
                         mode="lines+markers",
                         name="Starting Here"))
fig.add_trace(go.Scatter(x=monthly_day_trips.select(col("day")).rdd.flatMap(lambda x: x).collect(),
                         y=monthly_day_trips.select(col("avg_trips_ending_here")).rdd.flatMap(lambda x: x).collect(),
                         mode="lines+markers",
                         name="Ending Here"))
fig.update_layout(title="Average Daily Trip Trends by Day of Month for Lafayette St & E 8 St",
                  xaxis_title="Day of Month",
                  yaxis_title="Average Daily Trip Counts")
fig.show()

display(monthly_day_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Comment:
# MAGIC It can be observe that our station is mostly busy on 8th and 13th day of the month and less busy on 25th every month on average.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Comment:

# COMMAND ----------




# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px

daily_trips = trip_info.groupBy("date").agg(
    count(when(col("start_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_starting_here"),
    count(when(col("end_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_ending_here")
).orderBy("date")

daily_trips = daily_trips.withColumn("day_of_week", date_format(col("date"), "E"))
weekly_trips = daily_trips.groupBy("day_of_week").agg(
    avg(col("trips_starting_here")).alias("avg_trips_starting_here"),
    avg(col("trips_ending_here")).alias("avg_trips_ending_here")
).orderBy("day_of_week")

fig = px.bar(weekly_trips.toPandas(), x="day_of_week", y=["avg_trips_starting_here", "avg_trips_ending_here"], 
             labels={"day_of_week": "Day of the Week", "value": "Average Number of Trips", "variable": "Trips"}, 
             title="Average Daily Trip Trends for Lafayette St & E 8 St by Day of the Week")
fig.update_layout(
    plot_bgcolor='white',
    autosize=False,
    width=800,
    height=500,
    legend=dict(
        yanchor="top",
        y=0.99,
        xanchor="left",
        x=0.01
    ),
    font=dict(
        family="Arial",
        size=12,
        color="black"
    )
)
fig.show()



# COMMAND ----------

# MAGIC %md
# MAGIC ####Comment:
# MAGIC From the above figure, we can say that our station records the highest trip starting and ending on Wednesdays on average. Followed by Thurdays. This means that our station is very busy on Wednedays and Thursdays.

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



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Load the weather data
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))
display(weather_data)

# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px

# Load bike trip data and select relevant columns
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips')
trip_info = trip_info.select("ride_id", "started_at", "start_station_name")

# Filter for your assigned station
trip_info = trip_info.filter(col("start_station_name") == "Lafayette St & E 8 St")

# Convert to date
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Group by date
daily_trips = trip_info.groupBy("date").agg(count("ride_id").alias("num_trips"))
daily_trips = daily_trips.withColumnRenamed("num_trips", "num_trips")

# Load weather data and select relevant columns
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_data.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))

# # # Convert dt column to date and hour
#weather_data = weather_data.withColumn("date_weather", to_date(col("dt").cast("timestamp")))
weather_data = weather_data.withColumn("hour", hour(col("date_weather").cast("timestamp")))


# # # Join trip data and weather data
joined_data = daily_trips.join(weather_data, (daily_trips["date"] == weather_data["date_weather"]), "inner")
joined_data_select = joined_data.select("date_weather","num_trips", "temp", "feels_like", "pressure", "humidity", "wind_speed", "wind_deg", "description")


# from pyspark.sql.functions import col
import pandas as pd
# Cast temp, pressure, and humidity to double
joined_data_select = joined_data_select.withColumn("temp", col("temp").cast("double"))
joined_data_select = joined_data_select.withColumn("pressure", col("pressure").cast("double"))
joined_data_select = joined_data_select.withColumn("humidity", col("humidity").cast("double"))
joined_data_select = joined_data_select.withColumn("wind_speed", col("wind_speed").cast("double"))
joined_data_select = joined_data_select.toPandas()


# # # # # Plot number of trips against other weather variables
fig = px.scatter(joined_data_select, x="temp", y="num_trips",  hover_data=["date_weather"], title="Bike Trips vs. Date by Temperature")
fig.show()


fig = px.scatter(joined_data_select, x="humidity", y="num_trips",  hover_data=["date_weather"], title="Bike Trips vs. Date by Humidity")
fig.show()

fig = px.scatter(joined_data_select, x="date_weather", y="num_trips",  hover_data=["pressure"], title="Bike Trips vs. Date by Pressure" )
fig.show()

fig = px.scatter(joined_data_select, x="date_weather", y="num_trips",  hover_data=["wind_speed"], title="Bike Trips vs. Date by Wind Speed")
fig.show()

# fig = px.scatter(joined_data_select, x="description", y="num_trips", title="Bike Trips vs. Weather Description")
# fig.show()

# display(daily_trips)
#display(joined_data_select)

# COMMAND ----------

print(joined_data_select)
print(joined_data_select.dtypes)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
