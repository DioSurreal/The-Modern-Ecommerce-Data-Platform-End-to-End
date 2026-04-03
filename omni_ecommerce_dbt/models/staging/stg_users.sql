select
    id as user_id,
    first_name,
    last_name,
    email,
    age,
    gender,
    state,
    street_address,
    postal_code,
    city,
    country,
    latitude as user_lat,
    longitude as user_lng,
    traffic_source,
    created_at::timestamp_ntz as created_at
from {{ source('bronze', 'raw_users') }}