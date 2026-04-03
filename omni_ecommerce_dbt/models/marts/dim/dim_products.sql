{{ config(materialized='table') }}

select
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_brand,
    p.retail_price,
    p.department,
    dc.dc_name as dispatch_center_name
from {{ ref('stg_products') }} p
left join {{ ref('stg_distribution_centers') }} dc on p.dc_id = dc.dc_id