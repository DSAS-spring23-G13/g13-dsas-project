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

import os
# Read data from a CSV file in batch mode
weather_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load(NYC_WEATHER_FILE_PATH)

# Write the processed data to a Parquet file
output_path = GROUP_DATA_PATH + "historic_weather"

if not os.path.isdir(output_path):
    dbutils.fs.mkdirs(output_path)

weather_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

weather_df.write.format("delta").mode("overwrite").saveAsTable("historic_weather_info")

# verify the write
display(weather_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC use g13_db;
# MAGIC 
# MAGIC select top from historic_weather_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ## store historic data in group path

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/'))
# display(dbutils.fs.rm('dbfs:/FileStore/tables/G13/historic_weather', recurse = True))

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- SHOW DATABASES;
# MAGIC 
# MAGIC use g13_db;
# MAGIC -- drop table if exists weather_csv;
# MAGIC SHOW TABLES;

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
