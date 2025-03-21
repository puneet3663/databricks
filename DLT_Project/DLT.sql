-- Databricks notebook source

-- Creating the raw employee dataset (Bronze Layer)
CREATE INCREMENTAL LIVE TABLE employee_raw
COMMENT "The raw employee dataset, ingested from the landing zone."
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *, 
  reverse(split(_metadata.file_path, '/'))[0] as Source_Data_File, 
  date_format(from_utc_timestamp(now(), 'Australia/Sydney'), 'yyyy-MM-dd HH:mm:ss') as Record_Insertion_Date 
FROM cloud_files("/Volumes/puneet_catalog/puneet_schema/puneet_volume/employee/", "csv");

-- COMMAND ----------

-- Creating the raw department dataset (Bronze Layer)
CREATE INCREMENTAL LIVE TABLE department_raw
COMMENT "The raw department dataset, ingested from the landing zone."
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT 
  *,
  reverse(split(_metadata.file_path, '/'))[0] as Source_Data_File, 
  date_format(from_utc_timestamp(now(), 'Australia/Sydney'), 'yyyy-MM-dd HH:mm:ss') as Record_Insertion_Date 
FROM cloud_files("/Volumes/puneet_catalog/puneet_schema/puneet_volume/", "csv");

-- COMMAND ----------

-- Creating the cleaned employee dataset (Silver Layer)
CREATE INCREMENTAL LIVE TABLE employee_clean (
  CONSTRAINT age_cannot_be_negative EXPECT (age >= 0) ON VIOLATION DROP ROW
)
COMMENT "Employee dataset with cleaned-up datatypes / column names and age expectations."
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT * FROM stream(live.employee_raw);
