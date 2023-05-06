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

HISTORIC_BIKE_TRIPS = f"dbfs:/FileStore/tables/G13/historic_bike_trips/"

historic_bike_trips_df = spark.read.format('delta').load(HISTORIC_BIKE_TRIPS)

# Read the weather data
nyc_weather_df = spark.read.csv(
    "dbfs:/FileStore/tables/raw/weather/NYC_Weather_Data.csv",
    header=True,
    inferSchema=True
)

bike_status = f"dbfs:/FileStore/tables/G13/bronze/bike-status/"
bike_status_df = spark.read.format('delta').load(bike_status)

bike_station = f"dbfs:/FileStore/tables/bronze_station_info.delta"
bike_station_df = spark.read.format('delta').load(bike_station)

# COMMAND ----------

display(bike_station_df)

# COMMAND ----------

!pip install folium

# COMMAND ----------

# Your station name
your_station_name = 'Lafayette St & E 8 St'

# Filter the bike_station_df DataFrame to find the row with your_station_name
station_info = bike_station_df.filter(bike_station_df.name == your_station_name).take(1)[0]

# Extract the station_id value from the filtered row
your_station_id = station_info.station_id

print("Our Station ID:", your_station_id)

# COMMAND ----------

import folium

# Replace 'your_station_id' with the actual station_id you're interested in
# your_station_id = 'your_station_id'

# Find the latitude and longitude of your station
station_info = bike_station_df.filter(bike_station_df.station_id == your_station_id).take(1)[0]
station_latitude = station_info.lat
station_longitude = station_info.lon

# Create a Folium map centered at the station's location with the default "OpenStreetMap" map tile
m = folium.Map(location=[station_latitude, station_longitude], zoom_start=48)

# Create a custom icon for the marker with the color set to 'red'
icon = folium.Icon(icon='bicycle', prefix='fa', color='red')

# Add a marker for your station with the custom icon and popup text
folium.Marker([station_latitude, station_longitude], popup='Lafayette St & E 8 St', icon=icon).add_to(m)

# Display the map
m
