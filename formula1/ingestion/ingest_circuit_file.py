# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: read data from CSV to Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC When we use `inferschema`, spark job increses and that is very expensive if the data is very huge. 
# MAGIC that is why it should only be used in developement enviroment. 
# MAGIC 
# MAGIC for production application we should use below methods, it has two benifits 
# MAGIC 1. this way we are saving expensive operation by telling spark what we expect dataframe to be 
# MAGIC 2. if the dataframe is not what we expect it should give error and fail. 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType
from pyspark.sql.functions import col, current_timestamp,lit

circuit_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lang", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),    
])

circuits_df= (spark.read.option("header", True)
              .schema(circuit_schema)
                .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv"))

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You may notice that printSchema prints nullable as "true" for circuitId which we defined as False in the schema. This is standard behaviour fo the dataframe reader. If we need to handle nulls, that need to be handled explicitly using other functions.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: select only requird columns

# COMMAND ----------

circuits_selected_df= circuits_df.select(col('circuitId'),
 col('circuitRef'),
 col('name'),
 col('location'),
col('country'),
 col('lat'),
col('lang'),
 col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: rename columns to standard python format

# COMMAND ----------

circuits_renamed_df= circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lang","langitude") \
.withColumnRenamed("alt","altitude") \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: add new requied columns

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 5: Write data to datalake as parquet

# COMMAND ----------

#write it to processed mount 
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
