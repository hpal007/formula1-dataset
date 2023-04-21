# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType, DateType
from pyspark.sql.functions import concat,col,lit, current_timestamp


name_schema = StructType([
    StructField("forename", StringType(), False),
    StructField("surname", StringType(), False),
])
driver_schema = StructType([
    StructField("code", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("url", StringType(), True),
])

driver_df = spark.read.schema(driver_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")
driver_final_df = driver_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .withColumn("ingetion_date",current_timestamp()) \
    .withColumn("file_date", lit(v_file_date)) \
    .drop("url")
    

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.driver")

# COMMAND ----------

dbutils.notebook.exit("Success")
