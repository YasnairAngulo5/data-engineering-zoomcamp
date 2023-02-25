
# Creation of `fhv_tripdata` table
```
    CREATE OR REPLACE TABLE `dtc-de-375507.trips_data_all.fhv_tripdata` AS
    SELECT *
    FROM `dtc-de-375507.dezoomcamp.external_fhv_tripdata`
```

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- 41648442 
- 51648442
- 61648442  :white_check_mark:
- 71648442

To disabled the variable:
```
    dbt run --select stg_yellow_tripdata --var 'is_test_run: false'
    dbt run --select stg_green_tripdata --var 'is_test_run: false'
```
Then
```
    dbt run --select fact_trips
```

Query:
```
    SELECT COUNT(*) FROM `dtc-de-375507.production.fact_trips` 
    WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019,2020)
```

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- 89.9/10.1   :white_check_mark:
- 94/6
- 76.3/23.7
- 99.1/0.9

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- 33244696
- 43244696   :white_check_mark:
- 53244696
- 63244696

# Query
```
    SELECT COUNT(*) FROM `dtc-de-375507.production.stg_fhv_tripdata` 
    WHERE EXTRACT(YEAR FROM pickup_datetime ) = 2019
```

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- 12998722
- 22998722   :white_check_mark:
- 32998722
- 42998722

# Query
```
    SELECT COUNT(*) FROM `dtc-de-375507.production.fact_fhv_trips`
    WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December