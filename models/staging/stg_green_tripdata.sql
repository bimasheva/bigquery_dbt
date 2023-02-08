{{ config(materialized="view") }}

select *
from {{ source("staging", "green_data") }}
limit 100
