{{ config(materialized='table') }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

daily_metrics as (
    select
        date_trunc('day', created_at_ts) as report_date,
        sum(case when status not in ('Cancelled', 'Returned') then sale_price else 0 end) as gross_revenue,
        sum(case when status = 'Returned' then sale_price else 0 end) as returned_value,
        count(distinct order_id) as total_orders
    from order_items
    group by 1
)

select
    *,
    -- คำนวณ Profit Margin แบบสมมติ (Gross Rev - Returned)
    (gross_revenue - returned_value) as net_revenue,
    -- ตรวจสอบสุขภาพธุรกิจ (Return Rate %)
    case 
        when gross_revenue > 0 then (returned_value / gross_revenue) * 100 
        else 0 
    end as return_rate_pct
from daily_metrics
order by report_date desc