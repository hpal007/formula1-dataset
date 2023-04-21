# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all the data required 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

driver_df = spark.read.format("delta").load(f"{processed_folder_path}/driver")
race_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
circuit_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
constructor_df = spark.read.format("delta").load(f"{processed_folder_path}/constructor")
result_df = spark.read.format("delta").load(f"{processed_folder_path}/results")

# COMMAND ----------

from pyspark.sql.functions import to_date

driver_df = driver_df.withColumnRenamed("name","driver_name").withColumnRenamed("nationality","driver_nationality").withColumnRenamed("number","driver_number")

circuit_df = circuit_df.withColumnRenamed('country',"circuit_location")

race_df = race_df.withColumnRenamed("name","race_name").withColumn("race_date",to_date("race_timestamp"))

constructor_df = constructor_df.withColumnRenamed("name","team")
result_df= result_df.withColumnRenamed("time","race_time").withColumnRenamed("race_id", "result_race_id").withColumnRenamed("file_date","result_file_date")


# COMMAND ----------

race_circuit_join_df = race_df.join(circuit_df,race_df.circuit_id==circuit_df.circuit_id,"inner")\
    .select(race_df.race_id,race_df.race_year,race_df.race_date,race_df.race_name,circuit_df.circuit_location)

# COMMAND ----------

race_results_df = result_df.join(race_circuit_join_df, race_circuit_join_df.race_id==result_df.result_race_id) \
                           .join(driver_df,result_df.driver_id==driver_df.driver_id) \
                           .join(constructor_df, constructor_df.constructor_id ==result_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select('result_race_id','driver_name','driver_nationality','driver_number','fastest_lap','grid','points','race_time','race_year','race_date','race_name','circuit_location','team','position','result_file_date') \
    .withColumn("created_date",current_timestamp()).withColumnRenamed("result_race_id","race_id")

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT race_id, count(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------


