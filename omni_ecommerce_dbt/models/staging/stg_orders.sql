select
    order_id,
    user_id,
    status as order_status,
    gender as customer_gender,
    created_at::timestamp_ntz as created_at_ts,
    returned_at::timestamp_ntz as returned_at,
    shipped_at::timestamp_ntz as shipped_at,
    delivered_at::timestamp_ntz as delivered_at,
    num_of_item as item_count
from {{ source('bronze', 'raw_orders') }}