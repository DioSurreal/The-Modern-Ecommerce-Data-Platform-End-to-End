select
    id as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status as item_status,
    created_at::timestamp_ntz as created_at_ts,
    shipped_at::timestamp_ntz as shipped_at,
    delivered_at::timestamp_ntz as delivered_at,
    returned_at::timestamp_ntz as returned_at,
    sale_price
from {{ source('bronze', 'raw_order_items') }}