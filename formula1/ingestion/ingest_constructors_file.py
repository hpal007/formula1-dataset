# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING,url STRING"
constructor_df = spark.read.schema(constructor_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef","constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success")
