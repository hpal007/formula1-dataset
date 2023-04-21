# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")
print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType,FloatType
from pyspark.sql.functions import concat,col,lit, current_timestamp

pit_stop_schema = StructType([
    StructField("raceId",IntegerType(),True),
    StructField("driverId",IntegerType(),False),
    StructField("stop",StringType(),True),
    StructField("lap",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("duration",StringType(),True),
    StructField("milliseconds",IntegerType(),True), 
])

pit_stops_df = spark.read \
.schema(pit_stop_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")


pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId","driver_id") \
                               .withColumnRenamed("raceId","race_id") \
                               .withColumn("file_date", lit(v_file_date)) \
                               .withColumn("ingetion_date", current_timestamp())

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

--%sql
--DROP TABLE f1_processed.pit_stops

# COMMAND ----------

display(spark.read.format('delta').load(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


