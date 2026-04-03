select
    id as product_id,
    cost as product_cost,
    category as product_category,
    name as product_name,
    brand as product_brand,
    retail_price,
    department,
    sku,
    distribution_center_id as dc_id
from {{ source('bronze', 'raw_products') }}