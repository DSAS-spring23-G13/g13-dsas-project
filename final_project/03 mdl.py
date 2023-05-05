# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, from_unixtime

HISTORIC_BIKE_TRIPS = f"dbfs:/FileStore/tables/G13/historic_bike_trips/"

historic_bike_trips_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)
historic_bike_trips_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))

# Read the weather data
# nyc_weather_df = spark.read.csv(
#     "dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv",
#     header=True,
#     inferSchema=True
# )
nyc_weather = f"dbfs:/FileStore/tables/G13/historic_weather_info_v4"

nyc_weather_df = spark.read.format('delta').load(nyc_weather)

# Convert the 'dt' column from Unix timestamp to a date type column
nyc_weather_df = nyc_weather_df.withColumn("date", to_date(from_unixtime("dt")))

bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"
bike_status_df = spark.read.format('delta').load(bike_status)

bike_station = f"dbfs:/FileStore/tables/G13/bronze/bike-station-info/"
bike_station_df = spark.read.format('delta').load(bike_station)

# COMMAND ----------

bike_status_df.head(2)

# COMMAND ----------

display(historic_bike_trips_df)

# COMMAND ----------

# display(lafayette_df)

# COMMAND ----------

print(historic_bike_trips_df.count())

# COMMAND ----------

# from pyspark.sql.functions import to_date, date_format

# lafayette_df = historic_bike_trips_df
# # display(lafayette_df)
# print(lafayette_df.count())

# # Extract the hour from the 'started_at' column
# lafayette_df = lafayette_df.withColumn("hour", date_format("started_at", "yyyy-MM-dd HH:00:00"))
# print(lafayette_df.count())

# # Group by hour and calculate the net bike change
# hourly_trips = lafayette_df.groupBy("hour").agg(
#     (F.count(F.when(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))) -
#      F.count(F.when(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id")))).alias("net_bike_change")
# )

# # Order the result by hour
# hourly_trips = hourly_trips.orderBy("hour")

# # hourly_trips.count()
# print(lafayette_df.count())

# COMMAND ----------

from pyspark.sql.functions import to_date, date_format, isnull

lafayette_df = historic_bike_trips_df

# Drop rows with null values
lafayette_df = lafayette_df.dropna()

# Check for null values
# for column in lafayette_df.columns:
#     null_count = lafayette_df.filter(isnull(column)).count()
#     print(f"Null values in {column}: {null_count}")

# Extract the hour from the 'started_at' column
lafayette_df = lafayette_df.withColumn("hour", date_format("started_at", "yyyy-MM-dd HH:00:00"))

# Group by hour and calculate the net bike change
# hourly_trips = lafayette_df.groupBy("hour").agg(
#     (F.count(F.when(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))) -
#      F.count(F.when(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id")))).alias("net_bike_change")
# )

# Group by hour and calculate the net bike change
hourly_trips = lafayette_df.groupBy("hour").agg(
    F.abs(
        (F.count(F.when(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))) -
         F.count(F.when(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))))
    ).alias("net_bike_change")
)

# Order the result by hour
hourly_trips = hourly_trips.orderBy("hour")

# COMMAND ----------

from pyspark.sql.functions import to_date, date_format, isnull

# Check for null values
for column in hourly_trips.columns:
    null_count = hourly_trips.filter(isnull(column)).count()
    print(f"Null values in {column}: {null_count}")

# COMMAND ----------

# from pyspark.sql.functions import date_format, from_unixtime, col

# # Convert the 'dt' column from Unix timestamp to an hour type column
# nyc_weather_df = nyc_weather_df.withColumn("hour", date_format(from_unixtime("dt"), "yyyy-MM-dd HH:00:00"))

# # Extract date and time from the 'hour' column
# nyc_weather_df = nyc_weather_df.withColumn("date", date_format(col("hour"), "yyyy-MM-dd"))
# nyc_weather_df = nyc_weather_df.withColumn("time", date_format(col("hour"), "HH:mm:ss"))

# # Group by hour and select the required columns
# weather_hourly = nyc_weather_df.groupBy("hour", "date", "time").agg(
#     F.first("temp").alias("temperature"),
#     F.first("rain_1h").alias("precipitation"),
#     F.first("wind_speed").alias("wind_speed"),
#     F.first("main").alias("main")  # Select the first non-null weather condition in each hour
# )

# weather_hourly.show()

# COMMAND ----------

from pyspark.sql.functions import date_format, from_unixtime, col

# Convert the 'dt' column from Unix timestamp to an hour type column
nyc_weather_df = nyc_weather_df.withColumn("hour", date_format(from_unixtime("dt"), "yyyy-MM-dd HH:00:00"))

# Extract date and time from the 'hour' column
nyc_weather_df = nyc_weather_df.withColumn("date", date_format(col("hour"), "yyyy-MM-dd"))
nyc_weather_df = nyc_weather_df.withColumn("time", date_format(col("hour"), "HH:mm:ss"))

# Group by hour and select the required columns
weather_hourly = nyc_weather_df.groupBy("date", "hour", "time").agg(
    F.avg("temp").alias("temperature"),
    F.avg("rain_1h").alias("precipitation"),
    F.avg("wind_speed").alias("wind_speed"),
    F.first("main").alias("main")  # Select the first non-null weather condition in each hour
)

# Check for null values in the DataFrame
for column in weather_hourly.columns:
    null_count = weather_hourly.filter(weather_hourly[column].isNull()).count()
    print(f"Null values in {column}: {null_count}")

# Remove rows with null values
weather_hourly = weather_hourly.dropna()

# Show the DataFrame
weather_hourly.show()

# COMMAND ----------

print(weather_hourly.count())

# COMMAND ----------

# Join the hourly_trips and weather_hourly DataFrames on the 'date' and 'hour' columns
hourly_trips_weather = hourly_trips.join(weather_hourly, on=["hour"])

hourly_trips_weather.show()

# COMMAND ----------

hourly_trips_weather.count()

# COMMAND ----------

display(hourly_trips_weather)

# COMMAND ----------

hourly_trips_weather.count()

# COMMAND ----------

bike_status_df.head(1)

# COMMAND ----------

hourly_trips_weather.head()

# COMMAND ----------

hourly_trips_weather = hourly_trips_weather.drop("date", "time", "main")

# COMMAND ----------

hourly_trips_weather_pd = hourly_trips_weather.toPandas()

# COMMAND ----------

hourly_trips_weather_pd = hourly_trips_weather_pd.rename(columns={'hour': 'ds', 'net_bike_change': 'y'})

# COMMAND ----------

hourly_trips_weather_pd['ds'] = pd.to_datetime(hourly_trips_weather_pd['ds'])

# COMMAND ----------

hourly_trips_weather_pd.head(2)

# COMMAND ----------

# hourly_trips_weather_pd = hourly_trips_weather_pd.drop("date", axis=1)
# hourly_trips_weather_pd = hourly_trips_weather_pd.drop("time", axis=1)

# COMMAND ----------

train_data = hourly_trips_weather_pd.sample(frac=0.8, random_state=42)
test_data = hourly_trips_weather_pd.drop(train_data.index)

# COMMAND ----------

train_data

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

from fbprophet import Prophet

# Create a Prophet model
model = Prophet()

# Fit the model to the training data
model.fit(train_data)

# COMMAND ----------

# Create a future DataFrame for the test dataset
future = test_data.drop('y', axis=1)

# Make predictions
forecast = model.predict(future)

# COMMAND ----------

from fbprophet.plot import plot
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(15, 6))
ax.plot(test_data['ds'], test_data['y'], label='True Values', color='blue', linewidth=1)
fig = model.plot(forecast, ax=ax, xlabel='Date & Time', ylabel='Net Bike Change', plot_cap=True)
ax.legend(['True Values', 'Predictions'])
plt.title('Prophet Model Forecast')
plt.grid()
plt.show()

# COMMAND ----------

from fbprophet.plot import plot_components

fig = plot_components(model, forecast)
plt.show()

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_squared_error

y_true = test_data['y'].values
y_pred = forecast['yhat'].values

mae = mean_absolute_error(y_true, y_pred)
mse = mean_squared_error(y_true, y_pred)

print(f"Mean Absolute Error: {mae}")
print(f"Mean Squared Error: {mse}")

# COMMAND ----------

# Merge the true values from the test set with the forecasted values on the 'ds' column
true_vs_predicted = test_data.merge(forecast[['ds', 'yhat']], on='ds', how='inner')

# Rename the 'yhat' column to 'prediction'
true_vs_predicted = true_vs_predicted.rename(columns={'yhat': 'prediction'})

# Select the true values and predicted values for net bike change
true_vs_predicted = true_vs_predicted[["y", "prediction"]]

# Show the true values and predicted values
print(true_vs_predicted.head())

# COMMAND ----------

import pandas as pd

# Set the max number of rows displayed to a high value
pd.set_option("display.max_rows", 1000)

# Print the entire true_vs_predicted DataFrame
print(true_vs_predicted)

# COMMAND ----------

# Convert the 'ds' column in test_data and forecast to a datetime object
test_data['ds'] = pd.to_datetime(test_data['ds'])
forecast['ds'] = pd.to_datetime(forecast['ds'])

# Merge the true values from the test set with the forecasted values on the 'ds' column
true_vs_predicted = test_data.merge(forecast[['ds', 'yhat']], on='ds', how='inner')

# Rename the 'yhat' column to 'prediction'
true_vs_predicted = true_vs_predicted.rename(columns={'yhat': 'prediction'})

# Select the true values and predicted values for net bike change
true_vs_predicted = true_vs_predicted[["ds", "y", "prediction"]]

# Create a line chart comparing true values and predicted values over time
plt.figure(figsize=(15, 6))
plt.plot(true_vs_predicted['ds'], true_vs_predicted['y'], label='True Values', color='blue', linewidth=1)
plt.plot(true_vs_predicted['ds'], true_vs_predicted['prediction'], label='Predictions', color='red', linewidth=1)
plt.xlabel('Date & Time')
plt.ylabel('Net Bike Change')
plt.title('True Values vs Predictions')
plt.legend()
plt.grid()
plt.show()

# COMMAND ----------


