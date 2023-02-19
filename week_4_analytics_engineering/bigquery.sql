--Create external table from parquet files
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375507.dezoomcamp.external_green_tripdata`
    OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-375507/green/green_tripdata_2019-*.parquet', 'gs://dtc_data_lake_dtc-de-375507/green/green_tripdata_2020-*.parquet']
);

-- Create materialized table 
CREATE OR REPLACE TABLE `dtc-de-375507.trips_data_all.yellow_tripdata` AS
SELECT * FROM dtc-de-375507.dezoomcamp.external_yellow_tripdata
   