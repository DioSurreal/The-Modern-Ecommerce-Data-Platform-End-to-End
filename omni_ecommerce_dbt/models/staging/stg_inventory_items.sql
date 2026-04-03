select
    id as inventory_item_id,
    product_id,
    product_distribution_center_id as dc_id,
    cost,
    product_category,
    product_name,
    product_brand,
    product_retail_price,
    product_department,
    product_sku,
    created_at::timestamp_ntz as created_at,
    sold_at::timestamp_ntz as sold_at
from {{ source('bronze', 'raw_inventory_items') }}