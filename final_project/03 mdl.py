# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# start_date = str(dbutils.widgets.get('01.start_date'))
# end_date = str(dbutils.widgets.get('02.end_date'))
# hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
# promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

# print(start_date,end_date,hours_to_forecast, promote_model)
# print("YOUR CODE HERE...")

# COMMAND ----------

from pyspark.sql.functions import hour, dayofweek
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, from_unixtime

HISTORIC_BIKE_TRIPS = f"dbfs:/FileStore/tables/G13/historic_bike_trips/"

historic_bike_trips_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)
historic_bike_trips_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT).withColumn("year", F.year("started_at")).withColumn("month", F.month("started_at"))

# Read the weather data
nyc_weather_df = spark.read.csv(
    "dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv",
    header=True,
    inferSchema=True
)
# Convert the 'dt' column from Unix timestamp to a date type column
nyc_weather_df = nyc_weather_df.withColumn("date", to_date(from_unixtime("dt")))

# bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"
# bike_status_df = spark.read.format('delta').load(bike_status)

# bike_station = f"dbfs:/FileStore/tables/G13/bronze/bike-station-info/"
# bike_station_df = spark.read.format('delta').load(bike_station)

# COMMAND ----------

from pyspark.sql.functions import to_date, date_format

lafayette_df = historic_bike_trips_df.filter(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT)

# Extract the hour from the 'started_at' column
lafayette_df = lafayette_df.withColumn("hour", date_format("started_at", "yyyy-MM-dd HH:00:00"))

# Group by hour and calculate the net bike change
hourly_trips = lafayette_df.groupBy("hour").agg(
    (F.count(F.when(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))) -
     F.count(F.when(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id")))).alias("net_bike_change")
)

# Order the result by hour
hourly_trips = hourly_trips.orderBy("hour")

hourly_trips.show()

# COMMAND ----------

from pyspark.sql.functions import date_format, from_unixtime, col

# Convert the 'dt' column from Unix timestamp to an hour type column
nyc_weather_df = nyc_weather_df.withColumn("hour", date_format(from_unixtime("dt"), "yyyy-MM-dd HH:00:00"))

# Extract date and time from the 'hour' column
nyc_weather_df = nyc_weather_df.withColumn("date", date_format(col("hour"), "yyyy-MM-dd"))
nyc_weather_df = nyc_weather_df.withColumn("time", date_format(col("hour"), "HH:mm:ss"))

# Group by hour and select the required columns
weather_hourly = nyc_weather_df.groupBy("hour", "date", "time").agg(
    F.first("temp").alias("temperature"),
    F.first("rain_1h").alias("precipitation"),
    F.first("wind_speed").alias("wind_speed"),
    F.first("main").alias("main")  # Select the first non-null weather condition in each hour
)

weather_hourly.show()

# COMMAND ----------

# Join the hourly_trips and weather_hourly DataFrames on the 'date' and 'hour' columns
hourly_trips_weather = hourly_trips.join(weather_hourly, on=["hour"])

hourly_trips_weather.show()

# COMMAND ----------

hourly_trips_weather.head(3)

# COMMAND ----------

display(hourly_trips_weather)

# COMMAND ----------

hourly_trips_weather_pd = hourly_trips_weather.toPandas()

# COMMAND ----------

hourly_trips_weather_pd = hourly_trips_weather_pd.rename(columns={'hour': 'ds', 'net_bike_change': 'y'})

# COMMAND ----------

hourly_trips_weather_pd['ds'] = pd.to_datetime(hourly_trips_weather_pd['ds'])

# COMMAND ----------

hourly_trips_weather_pd.head(2)

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


