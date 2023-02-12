-- Q1
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375507.dezoomcamp.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp_yas/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

-- Q2
-- Copy the data from the external table to an internal table
CREATE OR REPLACE TABLE `dtc-de-375507.dezoomcamp.fhv_tripdata` AS
SELECT  * FROM `dtc-de-375507.dezoomcamp.external_fhv_tripdata` 

-- Query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
-- Scanning  0 MB data
SELECT count(distinct(Affiliated_base_number)) FROM `dtc-de-375507.dezoomcamp.external_fhv_tripdata` 
-- Scanning  317.94 MB data
SELECT count(distinct(Affiliated_base_number)) FROM `dtc-de-375507.dezoomcamp.fhv_tripdata`

--Q3
-- This query will process 638.9 MB when run.
SELECT COUNT(*) FROM `dtc-de-375507.dezoomcamp.fhv_tripdata` 
WHERE PUlocationID IS NULL
  AND DOlocationID IS NULL 

  
-- Q5
-- Creation of partioned table
--This query will process 2.25 GB when run.
CREATE OR REPLACE TABLE `dtc-de-375507.dezoomcamp.fhv_tripdata_partitoned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `dtc-de-375507.dezoomcamp.fhv_tripdata`;

-- Query to non-partitioned table
-- This query will process 647.87 MB when run.
SELECT DISTINCT(Affiliated_base_number) FROM `dtc-de-375507.dezoomcamp.fhv_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-01'

--Query to partitioned table
--This query will process 814.62 KB when run.
SELECT DISTINCT(Affiliated_base_number) FROM `dtc-de-375507.dezoomcamp.fhv_tripdata_partitoned_clustered`
WHERE EXTRACT( DATE FROM pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-01'
