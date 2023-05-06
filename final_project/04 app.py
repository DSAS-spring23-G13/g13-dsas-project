# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)

print("YOUR CODE HERE...")

# COMMAND ----------

## Files 
display(dbutils.fs.ls('dbfs:/FileStore/tables/G13'))

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

display(bike_trips)

# COMMAND ----------

pip install folium

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

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
