# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType,FloatType
from pyspark.sql.functions import concat,col,lit, current_timestamp

qualifying_schema = StructType([
    StructField("qualifyId",IntegerType(),False),
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("constructorId",IntegerType(),True),
    StructField("number",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("q1",StringType(),True), 
    StructField("q2",StringType(),True), 
    StructField("q3",StringType(),True), 
])

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")


qualifying_final_df = qualifying_df.withColumnRenamed("driverId","driver_id") \
                               .withColumnRenamed("raceId","race_id") \
                               .withColumnRenamed("constructorId","constructor_id") \
                               .withColumnRenamed("qualifyId","qualify_id") \
                               .withColumn("file_date", lit(v_file_date)) \
                               .withColumn("ingetion_date", current_timestamp())

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

dbutils.notebook.exit("Success")
