select
    id as dc_id,
    name as dc_name,
    latitude as dc_lat,
    longitude as dc_lng
from {{ source('bronze', 'raw_distribution_centers') }}