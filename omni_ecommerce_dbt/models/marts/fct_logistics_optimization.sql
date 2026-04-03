{{ config(materialized='table') }}

with joined_data as (
    select * from {{ ref('int_order_logistics_joined') }}
),

calculations as (
    select
        *,
        st_distance(
            st_makepoint(user_lng, user_lat),
            st_makepoint(dc_lng, dc_lat)
        ) / 1000 as distance_km,
    from joined_data
),

costed as (
    select
        *,
        round(2 + (distance_km * 0.05), 2) as estimated_shipping_cost
    from calculations
)

select
    *,
    round(sale_price - product_cost - estimated_shipping_cost, 2) as net_profit_margin
from costed
