{{ config(materialized='table') }}

select
    user_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    age,
    gender,
    state,
    city,
    country,
    user_lat,
    user_lng,
    traffic_source,
    created_at as registration_date
from {{ ref('stg_users') }}