# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType,FloatType
from pyspark.sql.functions import concat,col,lit, current_timestamp

results_schema = StructType([
    StructField("constructorId",IntegerType(),True),
    StructField("driverId",IntegerType(),True),
    StructField("fastestLap",IntegerType(),True),
    StructField("fastestLapSpeed",FloatType(),True),
    StructField("fastestLapTime",FloatType(),True),
    StructField("grid",IntegerType(),True),
    StructField("laps",IntegerType(),True),
    StructField("milliseconds",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("points",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("positionOrder",IntegerType(),True),    
    StructField("positionText",IntegerType(),True),
    StructField("raceId",IntegerType(),True),    
    StructField("rank",IntegerType(),True),
    StructField("resultId",IntegerType(),True),    
    StructField("statusId",IntegerType(),True),
    StructField("time",StringType(),True),    
])

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json")


results_final_df = results_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("ingetion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date)) \
.drop("statusId")

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

--%sql
--DROP TABLE f1_processed.results


# COMMAND ----------


