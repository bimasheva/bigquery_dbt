{{ config(materialized="view") }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source("staging", "fhv_tripdata_partitioned") }}
  where PUlocationID is not null  AND DOlocationID is not null
)

select 

    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as numeric) as sr_flag,
    dispatching_base_num as dispatching_basenum,
    Affiliated_base_number as affiliated_basenum,
    
from tripdata
where DATE(pickup_datetime) between "2019-01-01" AND "2019-12-31"
