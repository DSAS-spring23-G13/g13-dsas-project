# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

from pyspark.sql.functions import *
## Load the historic bikke data for Lafayette St & E 8 St
bike_trips = spark.read.format("delta")\
                .option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips')\
                    .filter((col("start_station_name")== "Lafayette t & E 8 St") | (col("end_station_name")== "Lafayette St & E 8 St"))\
                    .withColumn("start_date", to_date(col("started_at"), "yyyy-MM-dd"))\
                    .withColumn("end_date", to_date(col("ended_at"), "yyyy-MM-dd"))\
                    .withColumn("start_hour", date_format(col("started_at"), "HH:00"))\
                    .withColumn("end_hour", date_format(col("ended_at"), "HH:00"))\
                    .withColumn("trip_duration", from_unixtime(unix_timestamp("ended_at") - unix_timestamp("started_at"), "HH:mm:ss"))                

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.streaming import *
from pyspark.sql.window import Window
from datetime import datetime

# Read the stream from a source
streamDF = spark.read \
                  .format("delta") \
                  .load("dbfs:/FileStore/tables/bronze_station_status.delta").filter(col("station_id") =="66db65aa-0aca-11e7-82f6-3863bb44ef7c") \
                  .withColumn("last_reported",from_unixtime(col("last_reported")))
most_recent_status_date = streamDF.select(max(col("last_reported"))).first()[0]
print(most_recent_status_date)
current_bikes_available = streamDF.filter(col("last_reported") == most_recent_status_date).select(col("num_bikes_available")).first()[0]
print(current_bikes_available)


# COMMAND ----------

streamWeather = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/bronze_nyc_weather.delta").withColumn("dt_human",from_unixtime(col("dt"))).withColumnRenamed("rain.1h","rain_1h")
most_recent_weather_date = streamWeather.select(max(col("dt_human"))).first()[0]
current_temp = streamWeather.filter(col("dt_human") == most_recent_weather_date).select(col("temp")).first()[0]
current_precipitation = streamWeather.filter(col("dt_human") == most_recent_weather_date).select(col("rain_1h")).first()[0]
print(current_temp)
print(current_precipitation)

# COMMAND ----------

print(most_recent_weather_date)

# COMMAND ----------

from pyspark.sql.functions import col
station_info = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/bronze_station_info.delta").filter(col("station_id")=="66db65aa-0aca-11e7-82f6-3863bb44ef7c")
display(station_info)

# COMMAND ----------

from pyspark.sql.functions import col
station_info = spark.read.format("delta").option("header","true").load("dbfs:/FileStore/tables/bronze_station_status.delta").filter(col("station_id")=="66db65aa-0aca-11e7-82f6-3863bb44ef7c")
display(station_info)

# COMMAND ----------

!pip install folium

# COMMAND ----------

from datetime import datetime as dt
from datetime import timedelta
import json

dbutils.widgets.removeAll()

from datetime import datetime
currentDateAndTime = datetime.now()

from mlflow.tracking.client import MlflowClient
client = MlflowClient()

latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Staging"])
latest_staging_version = latest_version_info[0].version
latest_version_info = client.get_latest_versions(GROUP_MODEL_NAME, stages=["Production"])
latest_production_version = latest_version_info[0].version

dbutils.widgets.text('01.start_date', "2021-10-01")
dbutils.widgets.text('02.end_date', "2021-03-01")
dbutils.widgets.text('03.hours_to_forecast', '4')
dbutils.widgets.text('04.promote_model', 'No')
dbutils.widgets.text('05.current_timestamp', str(currentDateAndTime))
dbutils.widgets.text('06.production_version',str(latest_production_version))
dbutils.widgets.text('07.staging_version',str(latest_staging_version))
dbutils.widgets.text('08.station_name',str(GROUP_STATION_ASSIGNMENT))
dbutils.widgets.text('09.current_bikes_available',str(current_bikes_available))
dbutils.widgets.text('10.current_temperature',str(current_temp))
dbutils.widgets.text('11.current_precipitation',str(current_precipitation))

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = str(dbutils.widgets.get('04.promote_model'))

# COMMAND ----------

import folium

# Replace 'your_station_id' with the actual station_id you're interested in

# Find the latitude and longitude of your station
station_info = bike_trips.filter(bike_trips.end_station_id == '5788.13').take(1)[0]
station_latitude = station_info.end_lat
station_longitude = station_info.end_lng

# Create a Folium map centered at the station's location with the default "OpenStreetMap" map tile
m = folium.Map(location=[station_latitude, station_longitude], zoom_start=20)

# Create a custom icon for the marker with the color set to 'red'
icon = folium.Icon(icon='bicycle', prefix='fa', color='red')

# Add a marker for your station with the custom icon and popup text
folium.Marker([station_latitude, station_longitude], popup='Station Name - Lafayette St & E 8 St', icon=icon).add_to(m)

# Display the map
m

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/streaming'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/G13/streaming/bike_staus/output'))
import folium

# Replace 'your_station_id' with the actual station_id you're interested in

# Find the latitude and longitude of your station
station_info = bike_trips.filter(bike_trips.end_station_id == '5788.13').take(1)[0]
station_latitude = station_info.end_lat
station_longitude = station_info.end_lng

# Create a Folium map centered at the station's location with the default "OpenStreetMap" map tile
m = folium.Map(location=[station_latitude, station_longitude], zoom_start=20)

# Create a custom icon for the marker with the color set to 'red'
icon = folium.Icon(icon='bicycle', prefix='fa', color='red')

# Add a marker for your station with the custom icon and popup text
folium.Marker([station_latitude, station_longitude], popup='Station Name - Lafayette St & E 8 St', icon=icon).add_to(m)

# Display the map
m

# COMMAND ----------

from pyspark.sql.functions import col
bike_status = spark.read.load('dbfs:/FileStore/tables/G13/streaming/bike_staus/output').filter(col("station_id") == "66db65aa-0aca-11e7-82f6-3863bb44ef7c")
display(bike_status)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,bend_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
