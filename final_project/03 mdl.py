# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------



# COMMAND ----------

# read all CSV files from the directory into a single DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
weather_schema =  StructType([StructField('dt', IntegerType(), True), StructField('temp', DoubleType(), True), StructField('feels_like', DoubleType(), True), StructField('pressure', IntegerType(), True), StructField('humidity', IntegerType(), True), StructField('clouds', IntegerType(), True), StructField('visibility', IntegerType(), True), StructField('wind_speed', DoubleType(), True), StructField('wind_deg', IntegerType(), True), StructField('pop', DoubleType(), True), StructField('snow_1h', DoubleType(), True), StructField('main', StringType(), True), StructField('rain_1h', DoubleType(), True)])
historic_weather_df = spark.read.csv("dbfs:/FileStore/tables/raw/weather", header=True, schema = weather_schema)
historic_weather_df.write.format("delta").mode("overwrite").save('dbfs:/FileStore/tables/G13/historic_weather_info_v4')
display(historic_weather_df)

# COMMAND ----------

historic_weather.schema

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/tables/raw/weather/'))

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

from pyspark.sql.functions import to_date,col
from pyspark.sql.functions import year, month, concat_ws, date_format

historic_bike_trips_from  = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter("start_station_name == 'Lafayette St & E 8 St'").withColumn("start_date_column", to_date(col('started_at'))).withColumn("year_month", year(col('start_date_column'))*100 + month(col('start_date_column'))).withColumn("day_of_the_week", date_format(col("started_at"),"E")).withColumn("start_hour",date_format(col("started_at"),"HH:00"))

historic_bike_trips_to  = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_bike_trips').filter(" end_station_name == 'Lafayette St & E 8 St'").withColumn("end_date_column", to_date(col('ended_at'))).withColumn("year_month", year(col('end_date_column'))*100 + month(col('end_date_column'))).withColumn("day_of_the_week", date_format(col("ended_at"),"E")).withColumn("end_hour",date_format(col('ended_at'),"HH:00"))
#display(historic_bike_trips_to)

from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import col

historic_weather = spark.read.format("delta").option("header", "true").load('dbfs:/FileStore/tables/G13/historic_weather_info_v4').withColumn('human_timestamp', from_unixtime('dt')).withColumn("weather_hour",date_format(col('human_timestamp'),"HH:00")).withColumn("weather_date",to_date("human_timestamp"))
display(historic_weather)

# COMMAND ----------

from pyspark.sql.functions import count
historic_bike_trips_to_agg = historic_bike_trips_to.groupBy("end_date_column","end_hour").agg(count("ride_id").alias("num_rides_in")).orderBy("end_date_column","end_hour")
display(historic_bike_trips_to_agg)

# COMMAND ----------

from pyspark.sql.functions import count
historic_bike_trips_from_agg = historic_bike_trips_from.groupBy("start_date_column","start_hour").agg(count("ride_id").alias("num_rides_out")).orderBy("start_date_column","start_hour")
display(historic_bike_trips_from_agg)

# COMMAND ----------

from pyspark.sql.functions import avg
historic_weather_agg =  historic_weather.groupBy("weather_date","weather_hour").agg(avg("temp"), avg("feels_like"), avg("pressure"), avg("humidity"), avg("wind_speed"), avg("pop"),avg("snow_1h"), avg("rain_1h")).orderBy("weather_date","weather_hour")
display(historic_weather_agg)

# COMMAND ----------

trips_joined_df = historic_bike_trips_to_agg.join(historic_bike_trips_from_agg,
                             (col("end_date_column") == col("start_date_column"))
                             & (col("end_hour") == col("start_hour")),
                             "outer")
display(trips_joined_df)

# COMMAND ----------

from pyspark.sql.functions import coalesce

# Replace null values in "num_rides_in" and "num_rides_out" columns with 0
trips_joined_df = trips_joined_df.fillna({'num_rides_in': 0, 'num_rides_out': 0})

# Replace null values in "end_date_column" and "end_hour" columns with values from "start_date_column" and "start_hour", respectively
trips_joined_df = trips_joined_df.withColumn('end_date_column', coalesce('end_date_column', 'start_date_column'))
trips_joined_df = trips_joined_df.withColumn('end_hour', coalesce('end_hour', 'start_hour'))

# Replace null values in "start_date_column" and "start_hour" columns with values from "end_date_column" and "end_hour", respectively
trips_joined_df = trips_joined_df.withColumn('start_date_column', coalesce('start_date_column', 'end_date_column'))
trips_joined_df = trips_joined_df.withColumn('start_hour', coalesce('start_hour', 'end_hour'))
display(trips_joined_df)


# COMMAND ----------

trips_joined_df.columns

# COMMAND ----------

from pyspark.sql.functions import sum, when, col

trips_joined_df.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in trips_joined_df.columns]).show()


# COMMAND ----------

trips_joined_feat =trips_joined_df.select("start_date_column","start_hour","num_rides_in","num_rides_out").withColumn("net_change",col("num_rides_in")-col("num_rides_out"))
display(trips_joined_feat)

# COMMAND ----------

trips_joined_feat = trips_joined_feat.join(historic_weather_agg, (col("start_date_column") == col("weather_date"))
                             & (col("start_hour") == col("weather_hour")),
                             "left").dropna()
display(trips_joined_feat)

# COMMAND ----------

#from fbprophet import Prophet
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import concat, col, lit
trips_joined_feat = trips_joined_feat.withColumn("ds", concat(col("start_date_column"), lit(" "), col("start_hour")))
trips_joined_feat = trips_joined_feat.withColumn("ds", trips_joined_feat["ds"].cast("timestamp")).withColumn("y",col("net_change"))
model_df = trips_joined_feat.select("ds","avg(temp)","avg(pressure)","avg(humidity)","avg(wind_speed)","avg(pop)","avg(snow_1h)", "avg(rain_1h)","num_rides_in","num_rides_out", "y")
display(model_df)
model_df_pandas = model_df.toPandas()
model_df_pandas.drop(["num_rides_in","num_rides_out"],axis = 1, inplace=True)

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

from fbprophet import Prophet
from fbprophet import Prophet, serialize
from fbprophet.diagnostics import cross_validation, performance_metrics
#from fbprophet.hdays import USFederalHolidayCalendar, NYSHolidayCalendar
import holidays

# create a list of all US and NY holidays
us_holidays = holidays.US(years=[2020, 2021, 2022,2023])
ny_holidays = holidays.US(state='NY', years=[2020, 2021, 2022,2023])
all_holidays = us_holidays + ny_holidays

# convert the holidays to a pandas DataFrame
holidays_df = pd.DataFrame(list(all_holidays.items()), columns=['ds', 'holiday'])


# create Prophet object
m = Prophet(yearly_seasonality=True,  # include yearly seasonality
    seasonality_mode='multiplicative',  # use multiplicative seasonality
    daily_seasonality=True,  # do not include daily seasonality
    weekly_seasonality=True, 
    holidays=holidays_df) # do not include weekly seasonality)



m.add_seasonality(name='hourly', period=24, fourier_order=15, prior_scale=0.1)


# add regressor variables
m.add_regressor("avg(temp)")
m.add_regressor("avg(pressure)")
m.add_regressor("avg(humidity)")
m.add_regressor("avg(wind_speed)")
m.add_regressor("avg(pop)")
m.add_regressor("avg(snow_1h)")
m.add_regressor("avg(rain_1h)")

# fit the model to the Pandas DataFrame
#df = df.toPandas()
model_df_pandas.head()
#train = model_df_pandas[model_df_pandas["ds"] < "2022-03-01"]
#test = model_df_pandas[model_df_pandas["ds"] >= "2022-03-01"]
m.fit(model_df_pandas)

# Cross validation
baseline_model_cv = cross_validation(model=m, initial='420 days', horizon = '30 days', parallel="threads")
baseline_model_cv.head()

# Model performance metrics
baseline_model_p = performance_metrics(baseline_model_cv, rolling_window=1)
baseline_model_p.head()

# Get the performance value
#print(f"MAPE of baseline model: {baseline_model_p['mape'].values[0]}")


# COMMAND ----------

baseline_model_p

# COMMAND ----------

baseline_model_cv

# COMMAND ----------

import pandas as pd
future = m.make_future_dataframe(periods=24, freq='H')
future = pd.merge( left = future, right = model_df_pandas.drop(["y"],axis = 1), on = "ds", how = "left")
future.fillna(method="ffill",inplace=True)
future.head()

# COMMAND ----------

predictions = m.predict(future)
predictions.head()

# COMMAND ----------

m.plot(predictions)

# COMMAND ----------

import matplotlib.pyplot as plt
from fbprophet.plot import plot_components

fig = plot_components(m, predictions)
plt.show()

# COMMAND ----------

future.info()

# COMMAND ----------

trips_joined_feat.select([sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in trips_joined_feat.columns]).show()

# COMMAND ----------


import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
