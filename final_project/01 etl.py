# Databricks notebook source
# MAGIC %run "./includes/includes"

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
# print("YOUR CODE HERE...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## load historic data
# MAGIC BIKE_TRIP_DATA_PATH
# MAGIC NYC_WEATHER_FILE_PATH

# COMMAND ----------

# MAGIC %md
# MAGIC ## store historic data in group path

# COMMAND ----------

import os
weather_df = spark.read.format("csv").load(NYC_WEATHER_FILE_PATH)

destination_folder_path = GROUP_DATA_PATH + "historic_weather"

if not os.path.isdir(destination_folder_path):
    dbutils.fs.mkdirs(destination_folder_path)

weather_df.write.format("delta").mode("overwrite").option('dataChange', 'False').save(destination_folder_path)

weather_df.write.format("delta").mode("overwrite").saveAsTable("old_weather_info")

display(weather_df)

# COMMAND ----------

display(dbutils.fs.ls(destination_folder_path))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
