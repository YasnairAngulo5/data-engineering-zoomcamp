{{ config(materialized='view') }}

select -- identifiers
       {{ dbt_utils.surrogate_key(['pickup_datetime', 'dropOff_datetime']) }} as tripid,
       dispatching_base_num,
       Affiliated_base_number         as  affiliated_base_number,
       cast(PUlocationID as integer)  as  pickup_locationid,
       cast(DOlocationID as integer)  as  dropoff_locationid,
       
       -- timestamps
        pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,
        cast(SR_Flag as float64) AS sr_flag
from {{ source('staging','fhv_tripdata') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
       