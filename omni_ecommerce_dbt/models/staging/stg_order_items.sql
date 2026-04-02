with source as (
    select * from {{ source('ecommerce_raw', 'order_items') }}
)

select
    id as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    sale_price,
    cast(created_at as timestamp_ntz) as created_at_ts,
    cast(shipped_at as timestamp_ntz) as shipped_at_ts,
    cast(delivered_at as timestamp_ntz) as delivered_at_ts,
    cast(returned_at as timestamp_ntz) as returned_at_ts
from source