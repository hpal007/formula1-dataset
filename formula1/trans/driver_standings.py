# Databricks notebook source

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(f"result_file_date = '{v_file_date}'") 

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

driver_standing_df = race_results_df.groupBy("race_year","driver_name","team","driver_nationality") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1,True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')


# COMMAND ----------


