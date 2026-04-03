{{ config(materialized='view') }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

inventory as (
    select * from {{ ref('stg_inventory_items') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

dc as (
    select * from {{ ref('stg_distribution_centers') }}
),

joined as (
    select
        oi.order_id,
        oi.product_id,
        oi.sale_price,
        inv.cost as product_cost,
        u.user_lat,
        u.user_lng,
        dc.dc_lat,
        dc.dc_lng,
        dc.dc_name
    from order_items oi
    join inventory inv on oi.inventory_item_id = inv.inventory_item_id
    join users u on oi.user_id = u.user_id
    join dc on inv.dc_id = dc.dc_id
)

select * from joined
