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

## Load the data for our station - Lafayette St & E 8 St
from pyspark.sql.functions import *
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
display(trip_info)


# Load and filter the weather data
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info_v2').withColumn('timestamp', from_unixtime('dt')).withColumn("hour_weather",date_format(col('timestamp'),"HH:00")).withColumn("date_weather",to_date("timestamp"))
display(weather_data)


# COMMAND ----------

pip install fbprophet

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd

# Load the data for our station - Lafayette St & E 8 St
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", hour(to_utc_timestamp(col("started_at"), "EST")))
trip_info = trip_info.select("date", "hour", "start_station_name", "end_station_name")
trip_info = trip_info.filter(col("start_station_name") == "Lafayette St & E 8 St")
trip_info = trip_info.withColumn("net_bike_change", when(col("end_station_name") == "Lafayette St & E 8 St", -1).otherwise(1))

# Load and filter the weather data
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info_v2').withColumn('timestamp', from_unixtime('dt')).withColumn("hour_weather",date_format(col('timestamp'),"HH:00")).withColumn("date_weather",to_date("timestamp"))
weather_data = weather_data.select("date_weather", "hour_weather", "temp", "humidity","pressure", "wind_speed")

# Join trip data and weather data

# Merge the two dataframes by date
joined_df = trip_info.join(weather_data, (trip_info.date == weather_data.date_weather), "inner").drop("date")


# Group by date and hour
hourly_trips = joined_data.groupBy("date_weather", "hour_weather").agg(sum("net_bike_change").alias("net_bike_change"))
aggregate_df = hourly_trips.select(col("date_weather"), col("hour_weather"), col("net_bike_change")).toPandas()

aggregated_df = aggregate_df.rename(columns={"date_weather": "ds", "net_bike_change": "y"})
# Fit Prophet model

# prophet_df = pd.DataFrame(hourly_trips[["ds", "y"]])
display(aggregate_df)
# model = Prophet()
# model.fit(prophet_df)

# # Generate forecast for the next 24 hours
# future = model.make_future_dataframe(periods=24, freq="H")
# forecast = model.predict(future)

# # Plot forecast
# fig = model.plot(forecast)
# fig.show()


# COMMAND ----------

display(joined_df)

# COMMAND ----------

display(hourly_trips)

# COMMAND ----------

display(weather_data)

# COMMAND ----------

# Load and filter the weather data
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))
display(weather_data)

# COMMAND ----------

## Select the features to be used in buiilding the model
selected_trip_feat = trip_info.select(col('rideable_type'), col("member_casual"))
display(selected_trip_feat)

num_weather = ["temp", "pressure", "humidity","wind_speed","clouds" ]
selected_weather_feat = weather_data.select([col(c) for c in num_weather])
display(selected_weather_feat)



# COMMAND ----------

merged_df = selected_trip_feat.join(selected_weather_feat, how='inner')
display(merged_df)

# COMMAND ----------

from pyspark.sql.functions import to_date,col
from pyspark.sql.functions import year, month, concat_ws, date_format

historic_bike_trips_from  = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter("start_station_name == 'Lafayette St & E 8 St'").withColumn("start_date_column", to_date(col('started_at'))).withColumn("year_month", year(col('start_date_column'))*100 + month(col('start_date_column'))).withColumn("day_of_the_week", date_format(col("started_at"),"E")).withColumn("start_hour",date_format(col("started_at"),"HH:00"))
display(historic_bike_trips_from)

historic_bike_trips_to  = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(" end_station_name == 'Lafayette St & E 8 St'").withColumn("end_date_column", to_date(col('ended_at'))).withColumn("year_month", year(col('end_date_column'))*100 + month(col('end_date_column'))).withColumn("day_of_the_week", date_format(col("ended_at"),"E")).withColumn("end_hour",date_format(col('ended_at'),"HH:00"))
display(historic_bike_trips_to)

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col

historic_weather = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info_v4').withColumn('human_timestamp', from_unixtime('dt')).withColumn("weather_hour",date_format(col('human_timestamp'),"HH:00")).withColumn("weather_date",to_date("human_timestamp"))
display(historic_weather)

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd

historic_bike_trips = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips')
historic_bike_trips_lafayette = historic_bike_trips.filter("start_station_name == 'Lafayette St & E 8 St' OR end_station_name == 'Lafayette St & E 8 St'")
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("start_date_column", to_date(col('started_at')))
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("end_date_column", to_date(col('ended_at')))
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("year_month", year(col('start_date_column'))*100 + month(col('start_date_column')))
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("day_of_the_week", date_format(col("started_at"),"E"))
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("start_hour",date_format(col("started_at"),"HH:00"))
historic_bike_trips_lafayette = historic_bike_trips_lafayette.withColumn("end_hour",date_format(col("ended_at"),"HH:00"))

display(historic_bike_trips)


# COMMAND ----------

# Load the data for our station - Lafayette St & E 8 St
from pyspark.sql.functions import *
import pandas as pd
from fbprophet import Prophet


# Load the data for our station - Lafayette St & E 8 St
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", hour(to_utc_timestamp(col("started_at"), "EST")))
trip_info = trip_info.select("date", "hour", "start_station_name", "end_station_name")
trip_info = trip_info.filter(col("start_station_name") == "Lafayette St & E 8 St")
trip_info = trip_info.withColumn("net_bike_change", when(col("end_station_name") == "Lafayette St & E 8 St", -1).otherwise(1))

# Load and filter the weather data
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_data.withColumn("date_weather", date_format(to_utc_timestamp(col("dt"), "UTC"), "yyyy-MM-dd HH:mm:ss").cast("date"))
weather_data = weather_data.select("date_weather", "temp", "humidity", "wind_speed")

# Join trip data and weather data
joined_data = trip_info.join(weather_data, (trip_info.date == weather_data.date_weather), "left")

# Group by date and hour
hourly_trips = joined_data.groupBy("date", "hour").agg(sum("net_bike_change").alias("y")).withColumnRenamed("date", "ds")
hourly_trips = hourly_trips.toPandas()

# Fit Prophet model
model = Prophet()
model.add_regressor("temp")
model.add_regressor("pressure")
model.add_regressor("humidity")
model.add_regressor("wind_speed")
model.fit(hourly_trips)

# Generate forecast for the next 24 hours
future = model.make_future_dataframe(periods=24, freq="H")
future["temp"] = 55
future["pressure"] = 1013
future["humidity"] = 70
future["wind_speed"] = 5
forecast = model.predict(future)

# Plot forecast
fig = model.plot(forecast)
fig.show()

# COMMAND ----------

# Load the data for our station - Lafayette St & E 8 St
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))
trip_info = trip_info.withColumn("hour", hour(to_utc_timestamp(col("started_at"), "EST")))
trip_info = trip_info.select("date", "hour", "start_station_name", "end_station_name")
trip_info = trip_info.filter(col("start_station_name") == "Lafayette St & E 8 St")
trip_info = trip_info.withColumn("net_bike_change", when(col("end_station_name") == "Lafayette St & E 8 St", -1).otherwise(1))

# Load and filter the weather data
weather_data = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_data.withColumn("date_weather", date_format(to_utc_timestamp(col("dt"), "UTC"), "yyyy-MM-dd HH:mm:ss").cast("date"))
weather_data = weather_data.select("date_weather", "temp", "humidity", "wind_speed")


# Join trip data and weather data
joined_data = trip_info.join(weather_data, (trip_info.date == weather_data.date_weather), "left")

# Group by date and hour
hourly_trips = joined_data.groupBy("date", "hour").agg(sum("net_bike_change").alias("net_bike_change"))
hourly_trips = hourly_trips.toPandas()

# Fit Prophet model
hourly_trips.columns = ["ds", "hour", "y"]
prophet_df = pd.DataFrame(hourly_trips[["ds", "y"]])
model = Prophet()
model.fit(prophet_df)

# Generate forecast for the next 24 hours
future = model.make_future_dataframe(periods=24, freq="H")
forecast = model.predict(future)

# Plot forecast
fig = model.plot(forecast)
fig.show()


# COMMAND ----------

col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd

# Load the data for our station - Lafayette St & E 8 St
trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Group the data by hour and date_weather and calculate the mean
aggregated_df = df.groupby([pd.Grouper(key='date', freq='H')])['net_bike_change'].mean().reset_index()

# Set the index to the date_weather column
aggregated_df = aggregated_df.set_index('date')

# Convert the index to a datetime object
aggregated_df.index = pd.to_datetime(aggregated_df.index)

# Reset the index
aggregated_df = aggregated_df.reset_index(drop=False)

# Convert the PySpark DataFrame to a pandas DataFrame
aggregated_df = aggregated_df.toPandas()

# Set the index to the 'date_weather' column
aggregated_df = aggregated_df.set_index('date')

# Convert the index to a datetime object
aggregated_df.index = pd.to_datetime(aggregated_df.index)

# Reset the index
aggregated_df.reset_index(inplace=True)

# Rename the columns
aggregated_df = aggregated_df.rename(columns={"date": "ds", "net_bike_change": "y"})

# Fit the Prophet model
from prophet import Prophet
prophet_model = Prophet()
prophet_model.add_regressor("temp")
prophet_model.add_regressor("pressure")
prophet_model.add_regressor("humidity")
prophet_model.add_regressor("wind_speed")
prophet_model.add_regressor("clouds")
prophet_model.fit(aggregated_df)

# Make predictions
future = prophet_model.make_future_dataframe(periods=24, freq='H')
future = future.drop("y")
future = future.withColumn("temp", lit(55))
future = future.withColumn("pressure", lit(1013))
future = future.withColumn("humidity", lit(70))
future = future.withColumn("wind_speed", lit(5))
future = future.withColumn("clouds", lit(50))
forecast = prophet_model.predict(future)

# Display the forecast
display(forecast.select("ds", "yhat", "yhat_lower", "yhat_upper"))


# COMMAND ----------

display(merged_df)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
