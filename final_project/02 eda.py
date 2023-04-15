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

NYC_WEATHER_FILE_PATH
BIKE_TRIP_DATA_PATH
BRONZE_STATION_INFO_PATH
BRONZE_STATION_STATUS_PATH
BRONZE_NYC_WEATHER_PATH

old_weather_df = spark.read.format("csv").option("header", "true").load(NYC_WEATHER_FILE_PATH)
display(old_weather_df)

# COMMAND ----------

old_weather_df.printSchema()

# COMMAND ----------

old_bike_info_fs = [file.path for file in dbutils.fs.ls(BIKE_TRIP_DATA_PATH) if file.name.endswith("_citibike_tripdata.csv")]

old_bike_info_fs

# COMMAND ----------

# lets start with last years trend
oldb_df = spark.read.format("csv").option("header", "true").load('dbfs:/FileStore/tables/raw/bike_trips/202301_citibike_tripdata.csv')
display(oldb_df)

# COMMAND ----------

# unique rideable
unique_values = [row.rideable_type for row in oldb_df.select("rideable_type").distinct().collect()]
# unique_values # ['docked_bike', 'classic_bike', 'electric_bike']

# how about some commonly start start_station_name

# COMMAND ----------

dbutils.fs.ls(BIKE_TRIP_DATA_PATH)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
