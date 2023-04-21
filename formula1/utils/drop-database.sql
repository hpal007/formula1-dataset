-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/deformula1dl/presentation";

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/deformula1dl/processed";

-- COMMAND ----------


