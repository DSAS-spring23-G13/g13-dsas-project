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

from pyspark.sql.functions import *
## Load the historic bikke data for Lafayette St & E 8 St
bike_trips = spark.read.format("delta")\
                .option("header", "true").load(G13_BRONZE_BIKE_TRIP)\
                    .filter((col("start_station_name")== GROUP_STATION_ASSIGNMENT) | (col("end_station_name") == GROUP_STATION_ASSIGNMENT))\
                    .withColumn("start_date", to_date(col("started_at"), "yyyy-MM-dd"))\
                    .withColumn("end_date", to_date(col("ended_at"), "yyyy-MM-dd"))\
                    .withColumn("start_hour", date_format(col("started_at"), "HH:00"))\
                    .withColumn("end_hour", date_format(col("ended_at"), "HH:00"))\
                    .withColumn("trip_duration", from_unixtime(unix_timestamp("ended_at") - unix_timestamp("started_at"), "HH:mm:ss"))

display(bike_trips)

# COMMAND ----------

## Data summary
display(bike_trips.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Note:
# MAGIC This analysis will first answer questions in the rubrics and other exploratory analysis will be performed to provide more visuals about the data.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. What are the monthly trip trends for your assigned station?

# COMMAND ----------

## Group by month and count to produce number of trips for each month
import plotly.express as px

monthly_trips = bike_trips.groupBy(month(col("start_date")).alias("month")).agg(
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
# MAGIC ##### Comment:

# COMMAND ----------

import plotly.express as px

# Group the data by year and month and count the number of trips
monthly_trips = bike_trips.withColumn("year_month", year(col("start_date")) * 100 + month(col("start_date"))) \
                               .groupBy("year_month").count() \
                               .orderBy("year_month")

# Convert the year and month columns into a single year_month column
monthly_trips = monthly_trips.withColumn("year_month", to_date(col("year_month"), "yyyyMM")) \
                             .withColumn("year_month", date_format(col("year_month"), "MMM-yyyy"))

display(monthly_trips)

##  Visualize the data using a bar chart
fig = px.line(monthly_trips.toPandas(), x='year_month', y= "count",
              labels={'year_month':'Year_Month', 'count':'Frequency of trips', 'variable':'Key'},
              title='Monthly Trip Trends for Lafayette St & E 8 St Station')
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Comment:
# MAGIC
# MAGIC The line chart presents an overview of the monthly trip trends at Lafayette St & E 8 St station from November 2021 to March 2023. It reveals an upward trend in the number of trips from the station, although there were some fluctuations.
# MAGIC
# MAGIC There was a marked increase in the number of trips between May and July of 2022, which continued until August 2022, after which there was a gradual decline until November 2022, followed by a sudden drop in December 2022. The decline in trips during December 2022 may have been due to the winter season and associated weather conditions. It is also interesting to note that there was a similar pattern in 2021, with a sharp decline in trips around December and February, which is likely to have been influenced by similar weather conditions.
# MAGIC
# MAGIC The data indicates a seasonal pattern in the trip trends, with more trips during the summer months (June to August) and fewer trips during the winter months (December to February). However, this trend was not consistent throughout the entire period, as there were other factors that may have influenced the trip trends.
# MAGIC
# MAGIC The findings suggest that there may be a potential demand for bike trips at Lafayette St & E 8 St Station, and further investigation is needed to identify the underlying factors that drive the trip trends. Such factors may include weather conditions, events, and other socio-economic factors. A deeper understanding of these factors can inform strategies to optimize bike sharing services and enhance customer experience.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. What are the daily trip trends for your given station?

# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px
import plotly.graph_objects as go


# group by day and calculate daily trip counts
daily_trips = bike_trips.groupBy(col("start_date")).agg(
    count(when(col("start_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_starting_here"),
    count(when(col("end_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_ending_here")
).orderBy(col("start_date"))

# group by day of month and calculate daily trip counts
monthly_day_trips = daily_trips.groupBy(dayofmonth(col("start_date")).alias("day")).agg(
    sum(col("trips_starting_here")).alias("total_trips_starting_here"),
    sum(col("trips_ending_here")).alias("total_trips_ending_here")
).orderBy(col("day"))

# plot daily trip counts by day of month
fig = go.Figure()
fig.add_trace(go.Scatter(x=monthly_day_trips.select(col("day")).rdd.flatMap(lambda x: x).collect(),
                         y=monthly_day_trips.select(col("total_trips_starting_here")).rdd.flatMap(lambda x: x).collect(),
                         mode="lines+markers",
                         name="Starting Here"))
fig.add_trace(go.Scatter(x=monthly_day_trips.select(col("day")).rdd.flatMap(lambda x: x).collect(),
                         y=monthly_day_trips.select(col("total_trips_ending_here")).rdd.flatMap(lambda x: x).collect(),
                         mode="lines+markers",
                         name="Ending Here"))
fig.update_layout(title="Daily Trip Trends by Day of the Month for Lafayette St & E 8 St",
                  xaxis_title="Day of Month",
                  yaxis_title="Daily Trip Counts")
fig.show()

display(monthly_day_trips)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comment:
# MAGIC The plot shows the daily trip trends by day of the month for Lafayette St & E 8 St station. The graph displays two lines: one showing the trend of daily trips starting at the station and the other showing the trend of daily trips ending at the station.
# MAGIC
# MAGIC From the graph, it can be observed that the number of trips ending at the station is higher than the number of trips starting at the station, indicating that the station is more frequently used as a starting point for bike trips than as an endpoint.
# MAGIC
# MAGIC There is a general trend of a higher number of trips starting and ending at the station during the weekdays (days 1-5) as compared to the weekends (days 6-9). This suggests that the station is used more frequently for commuting and work-related trips during the weekdays and for leisure activities during the weekends.
# MAGIC
# MAGIC Furthermore, there are two distinct peaks in the number of trips starting and ending at the station, one around the 13th day of the month and the other around the 20th day of the month. These peaks could be attributed to events or activities that typically occur around those days, such as monthly pay cycles or scheduled events in the area.
# MAGIC
# MAGIC Overall, this graph provides insights into the usage patterns of the Lafayette St & E 8 St station, which could be useful for optimizing bike supply and demand in the area.

# COMMAND ----------

from pyspark.sql.functions import *
import plotly.express as px

# Count the number of trips starting and ending at each hour of the day
hourly_trips = bike_trips.groupBy(hour(col("started_at")).alias("start_hour"), hour(col("ended_at")).alias("end_hour")) \
    .agg(count(col("ride_id")).alias("trip_count")) \
    .orderBy(col("start_hour"), col("end_hour"))

# Pivot the data to create a heatmap of trip counts by start and end hours
heatmap_data = hourly_trips.groupBy(col("start_hour")).pivot("end_hour").agg(first(col("trip_count"))).orderBy(col("start_hour"))
heatmap_data = heatmap_data.na.fill(0)

# Create the bar graph
fig = px.bar(hourly_trips.toPandas(),
             x=["start_hour", "end_hour"],
             y="trip_count",
             color="start_hour",
             title="Hourly Trip Counts by Start and End Hour",
             labels={"start_hour": "Hour", "end_hour": "End Hour", "trip_count": "Trip Count"})
fig.update_layout(title="Hourly Trip Trends for Lafayette St & E 8 St",
                  xaxis_title="Hours (in 24hrs)",
                  yaxis_title="Trip Counts",
                  xaxis=dict(tickvals=list(range(24)),
                             ticktext=[f"{h:02d}:00" for h in range(24)]))
fig.show()

display(hourly_trips)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Comment:
# MAGIC The hourly trips graph shows the number of bike trips starting and ending at each hour of the day for Lafayette St & E 8 St station. It can be observed that the station is busiest during the morning rush hour, between 8:00 and 9:00, with more bikes ending their trips at the station, possibly due to people commuting to work or running errands in the area. Additionally, there is a high frequency of bike trips starting from the station during this period.
# MAGIC
# MAGIC The graph also shows a high volume of bike trips during the evening rush hour, between 17:00 and 18:00, with similar frequencies for both starting and ending trips. This suggests that many people are using the station to commute from work or other activities during this time. Overall, the hourly trips graph provides insight into the station's usage patterns, which can be useful for optimizing bike placement, scheduling maintenance, and improving the overall user experience. 

# COMMAND ----------

### Days with the highest starting and ending trip frequencies
from pyspark.sql.functions import *
import plotly.express as px

daily_trips = bike_trips.groupBy("start_date").agg(
    count(when(col("start_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_starting_here"),
    count(when(col("end_station_name") == "Lafayette St & E 8 St", 1)).alias("trips_ending_here")
).orderBy("start_date")

daily_trips = daily_trips.withColumn("day_of_week", date_format(col("start_date"), "E"))
weekly_trips = daily_trips.groupBy("day_of_week").agg(
    sum(col("trips_starting_here")).alias("total_trips_starting_here"),
    sum(col("trips_ending_here")).alias("total_trips_ending_here")
).orderBy(when(col("day_of_week") == "Sun", 1).when(col("day_of_week") == "Mon", 2).when(col("day_of_week") == "Tue", 3).when(col("day_of_week") == "Wed", 4).when(col("day_of_week") == "Thu", 5).when(col("day_of_week") == "Fri", 6).when(col("day_of_week") == "Sat", 7)).alias("day_of_week_num")



fig = px.bar(weekly_trips.toPandas(), x="day_of_week", y=["total_trips_starting_here", "total_trips_ending_here"], 
             labels={"day_of_week": "Day of the Week", "value": "Number of Trips", "variable": "Trips"}, 
             title="Daily Trip Trends for Lafayette St & E 8 St by Day of the Week")
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

display(daily_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comment:
# MAGIC The above code generates a bar chart that displays the daily trip trends for our station (Lafayette St & E 8 St) by day of the week. The chart has two bars for each day of the week, one representing the total number of trips starting at that station and the other representing the total number of trips ending at that station. The x-axis shows the day of the week, and the y-axis shows the number of trips.
# MAGIC
# MAGIC Based on the chart, it appears that the Lafayette St & E 8 St station experiences the highest starting and ending trip frequencies on weekdays, with the peak occurring on Wednesdays indicating it as the busiest day in the week and a slight drop on Thursdays. There is a significant drop in trip frequency on weekends. The chart shows that trip frequency starts to increase after Sunday, reaches a maximum on Wednesday, and then starts to fall.

# COMMAND ----------

# MAGIC %md 
# MAGIC 3. How does a holiday affect the daily (non-holiday) system use trend?

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

import pandas as pd
from pyspark.sql.functions import *

# Aggregate the trips by day of the week
daily_trips = bike_trips.groupBy("start_date").agg(count("*").alias("trip_count")).orderBy("start_date") 
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

# Add a new column to the daily_trips DataFrame to indicate if the date is a holiday or not
daily_trips = daily_trips.withColumn("is_holiday", is_holiday("start_date"))

# Calculate the total trip counts for holidays and non-holidays
total_trips = daily_trips.groupBy("is_holiday").agg(sum("trip_count").alias("total_trip_count"))

total_trips.show()

total_trips_pd = total_trips.toPandas()

# Create a bar plot
fig = px.bar(total_trips_pd, x="is_holiday", y="total_trip_count", color="is_holiday",
             color_discrete_map={'False': 'blue', 'True': 'green'})

# Set the plot title, x-label, and y-label
fig.update_layout(title="Total Daily Trips by Holiday Status for Lafayette St & E 8 St", xaxis_title="Is Holiday?", yaxis_title="Total Trip Count")

# Customize the x-axis tick labels
fig.update_xaxes(tickvals=[False, True], ticktext=['Non-Holiday', 'Holiday'])

# Display the plot
fig.show()


# COMMAND ----------


#Create a user-defined function (UDF) to check if a date is a holiday
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import plotly.graph_objects as go

@udf(returnType=BooleanType())
def is_holiday(date):
    return str(date) in holiday_set

#Add a new column to the daily_trips DataFrame to indicate if the date is a holiday or not
daily_trips = daily_trips.withColumn("is_holiday", is_holiday("start_date"))

#Group the daily trips by date and holiday status, and calculate the total trip counts
total_trips = daily_trips.groupBy("start_date", "is_holiday").agg(sum("trip_count").alias("total_trip_count"))

#Filter the total trips to only include non-holidays
non_holiday_trips = total_trips.filter(col("is_holiday") == False).select("start_date", "total_trip_count")

#Filter the total trips to only include holidays
holiday_trips = total_trips.filter(col("is_holiday") == True).select("start_date", "total_trip_count")

#Convert the Spark DataFrames to Pandas DataFrames
non_holiday_trips_pd = non_holiday_trips.toPandas()
holiday_trips_pd = holiday_trips.toPandas()

#Create a line chart with two traces: one for holiday trip counts and one for non-holiday trip counts
fig = go.Figure()

fig.add_trace(go.Scatter(x=non_holiday_trips_pd['start_date'], y=non_holiday_trips_pd['total_trip_count'],
name='Non-Holiday', mode='lines'))

fig.add_trace(go.Scatter(x=holiday_trips_pd['start_date'], y=holiday_trips_pd['total_trip_count'],
name='Holiday', mode='lines'))

#Set the plot title, x-axis label, and y-axis label
fig.update_layout(title="Daily Trip Counts by Holiday Status for Lafayette St & E 8 St", xaxis_title="Date", yaxis_title="Total Trip Count")

#Display the plot
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Comment:
# MAGIC The line chart that we generated shows two traces, one for holiday trip counts and one for non-holiday trip counts. The x-axis represents dates and the y-axis represents trip counts. The chart title, x-axis label, and y-axis label are specified in the layout. The chart allows us to compare the daily trend in system use between holidays and non-holidays.
# MAGIC
# MAGIC Looking at the chart, we can see that the daily trend in system use is generally higher on non-holidays than on holidays. This suggests that people use the system more on non-holidays, possibly due to work or school commutes. On the other hand, holidays may be associated with lower levels of system use, possibly because people have the day off or are engaged in other activities.
# MAGIC
# MAGIC Additionally, we can see that the trip counts on holidays are much more sporadic and volatile than non-holidays. This suggests that system usage is more erratic during holidays, possibly due to people engaging in a wider variety of activities or having less structured schedules.
# MAGIC
# MAGIC Overall, this chart provides valuable insights into how holidays affect the daily trend in system use. It suggests that holidays are associated with lower levels of system use and more erratic patterns of usage compared to non-holidays. This information can be used by system operators to better manage resources and plan for changes in demand during holiday periods.

# COMMAND ----------

##Explore ridership patterns by membership type

import plotly.graph_objs as go
from pyspark.sql.functions import count, col

# Group by membership type and count the number of trips
ridership_by_membership = bike_trips.groupBy("member_casual").agg(count("ride_id").alias("trip_count"))

# Create a pie chart of ridership by membership type
fig = go.Figure(data=[go.Pie(labels=ridership_by_membership.select("member_casual").rdd.flatMap(lambda x: x).collect(),
                             values=ridership_by_membership.select("trip_count").rdd.flatMap(lambda x: x).collect())])
fig.update_layout(title="Ridership by Membership Type for Lafayette St & E 8 St")

# Show the plot
fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Comment:
# MAGIC The plot above shows the distribution of bike trips between members and casual riders, represented as a proportion of the total number of trips. It is evident that the majority of bike trips were taken by members, accounting for around 83% of all trips. In contrast, casual riders account for approximately 17% of all trips.
# MAGIC
# MAGIC This information is crucial in understanding the differences in ridership patterns between members and casual riders. For example, it suggests that casual riders may be more price-sensitive and less committed to bike sharing, while members may be more loyal to the service and willing to pay for a membership. This insight can inform strategic decisions around pricing strategies, membership benefits, and targeted marketing campaigns to attract and retain members.
# MAGIC
# MAGIC Overall, this analysis highlights the importance of understanding ridership patterns in making informed decisions that can improve the bike sharing service and enhance customer experience.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. How does weather affect the daily/hourly trend of system use?

# COMMAND ----------

## Identify popular routes

from pyspark.sql.functions import count, concat_ws

# Filter routes that involve the "Lafayette St & E" station as either start or end
lafayette_routes = bike_trips.filter((bike_trips.start_station_name.like("%Lafayette St & E%")) | (bike_trips.end_station_name.like("%Lafayette St & E%")))

# Filter out routes where start and end stations are the same
filtered_routes = lafayette_routes.filter(lafayette_routes.start_station_name != lafayette_routes.end_station_name)

# Group by start and end stations and count the number of trips
routes_df = filtered_routes.groupBy(concat_ws(" - ", "start_station_name", "end_station_name")).agg(count("ride_id").alias("trip_count"))

# Show the top 10 most popular routes
popular_routes = routes_df.orderBy("trip_count", ascending=False).limit(10)
display(popular_routes)

# COMMAND ----------

## Popular Routes in NYC

from pyspark.sql.functions import count, concat_ws
import plotly.express as px

# Filter routes that involve the "Lafayette St & E" station as either start or end
lafayette_routes = bike_trips.filter((bike_trips.start_station_name.like("%Lafayette St & E%")) | (bike_trips.end_station_name.like("%Lafayette St & E%")))

# Filter out routes where start and end stations are the same
filtered_routes = lafayette_routes.filter(lafayette_routes.start_station_name != lafayette_routes.end_station_name)

# Group by start and end stations and count the number of trips
routes_df = filtered_routes.groupBy(concat_ws(" - ", "start_station_name", "end_station_name")).agg(count("ride_id").alias("trip_count"))

# Show the top 10 most popular routes
popular_routes = routes_df.orderBy("trip_count", ascending=False).limit(10)

# Create a bar chart of the top 10 most popular routes
fig = px.bar(popular_routes.toPandas(), x="trip_count", y="concat_ws( - , start_station_name, end_station_name)", orientation="h")

# Add titles and labels to the chart
fig.update_layout(title="Top 10 Most Popular Bike Routes involving Lafayette St & E in New York City",
                  xaxis_title="Number of Trips",
                  yaxis_title="Route")

fig.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #####Comments:
# MAGIC
# MAGIC The plot shows the top 10 most popular bike routes involving the "Lafayette St & E" station in New York City. The x-axis represents the number of trips recorded for each route, while the y-axis represents the route name.
# MAGIC
# MAGIC From the plot, we can see that the route with the highest number of trips is "Cleveland Pl & Spring St - Lafayette St & E". This indicates that it is the most popular route among riders who either start or end their trips at the "Lafayette St & E" station.
# MAGIC Other popular routes include "Lafayette St & Jersey St - Lafayette St & E 8 St", "Broadway & E 14 St - Lafayette St & E 8 St", and "E 10 St & Avenue A - Lafayette St & E 8 St". These routes also have a significant number of trips, suggesting a high demand for bike rides between these locations.
# MAGIC
# MAGIC The plot provides a visual representation of the popularity of different routes involving the "Lafayette St & E" station, allowing for quick and easy interpretation of the data. This information can be valuable for urban planning, transportation management, and bike-sharing system optimization.

# COMMAND ----------

from pyspark.sql.functions import *
hist_weather_data = spark.read.format("delta").load(G13_BRONZE_WEATHER)\
            .withColumn("weather_hour",date_format(col('dt'),"HH:00"))\
                .withColumn("weather_date",to_date("human_timestamp"))

display(hist_weather_data)

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
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
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info_v2').withColumn('timestamp', from_unixtime('dt')).withColumn("hour_weather",date_format(col('timestamp'),"HH:00")).withColumn("date_weather",to_date("timestamp"))

# # # Convert dt column to date and hour
#weather_data = weather_data.withColumn("date_weather", to_date(col("dt").cast("timestamp")))
# weather_data = weather_data.withColumn("hour", hour(col("date_weather").cast("timestamp")))


# # # Join trip data and weather data
joined_data = daily_trips.join(weather_data, (daily_trips["date"] == weather_data["date_weather"]), "right")
joined_data_select = joined_data.select("date_weather","num_trips", "temp", "feels_like", "pressure", "humidity", "wind_speed", "wind_deg")
joined_data_select = joined_data_select.toPandas()


# # # # # Plot number of trips against other weather variables
fig = px.scatter(joined_data_select, x="date_weather", y="num_trips",  hover_data=["temp"], title="Bike Trips vs. Date by Temperature")
fig.show()


fig = px.scatter(joined_data_select, x="date_weather", y="num_trips",  hover_data=["humidity"], title="Bike Trips vs. Date by Humidity")
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

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
