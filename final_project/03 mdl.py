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
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))
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

# Load the data for our station - Lafayette St & E 8 St
from pyspark.sql.functions import *
import pandas as pd


trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", to_date(col("started_at"), "yyyy-MM-dd"))

# Load and filter the weather data
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))

# Merge the two dataframes by date
merged_df = trip_info.join(weather_data, (trip_info.date == weather_data.date_weather), "inner").drop("date_weather")

# Add an hour column to the merged_df
merged_df = merged_df.withColumn("hour", hour(col("date")))

merged_df = merged_df.toPandas()


# Reset the index
aggregated_df.reset_index(inplace=True)

# Rename the columns
aggregated_df = aggregated_df.withColumnRenamed("date_weather", "ds").withColumnRenamed("net_bike_change", "y")

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

# Load the data for our station - Lafayette St & E 8 St
from pyspark.sql.functions import *
import pandas as pd


trip_info = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter((col("start_station_name")== "Lafayette St & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))
trip_info = trip_info.withColumn("date", date_format(col("started_at"), "yyyy-MM-dd HH:00:00"))

# Load and filter the weather data
weather_info= spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info')
weather_data = weather_info.withColumn("date_weather", to_utc_timestamp(from_unixtime(col("dt")), "UTC"))

# Merge the two dataframes by date
merged_df = trip_info.join(weather_data, (trip_info.date == weather_data.date_weather), "inner").drop("date_weather")

# Add an hour column to the merged_df
merged_df = merged_df.withColumn("hour", hour(col("date")))

#merged_df = merged_df.toPandas()

# # Group data by date and hour columns
# aggregated_df = aggregated_df.set_index('date')
# aggregated_df.index = pd.to_datetime(aggregated_df.index)

aggregate_df = trip_info.groupBy("hour").agg(
    (F.count(F.when(F.col("start_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id"))) -
     F.count(F.when(F.col("end_station_name") == GROUP_STATION_ASSIGNMENT, F.col("ride_id")))).alias("net_bike_change")
)
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
future["temp"] = 55
future["pressure"] = 1013
future["humidity"] = 70
future["wind_speed"] = 5
future["clouds"] = 50
forecast = prophet_model.predict(future)

# Display the forecast
display(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])


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
