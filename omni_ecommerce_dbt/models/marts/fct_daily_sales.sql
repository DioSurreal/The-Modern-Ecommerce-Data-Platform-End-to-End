{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

daily_summary as (
    select
        date_trunc('day', created_at_ts) as sales_date,
        count(distinct order_id) as total_orders,
        count(distinct user_id) as unique_customers,
        sum(item_count) as total_items_sold,
        count(case when order_status = 'Returned' then 1 end) as total_returns
    from orders
    group by 1
)

select 
    *,
    lag(total_orders) over (order by sales_date) as previous_day_orders
from daily_summary
order by sales_date desc
