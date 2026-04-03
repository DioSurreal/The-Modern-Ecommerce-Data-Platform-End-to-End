{{ config(materialized='table') }}

with users as (
    select * from {{ ref('stg_users') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

user_value as (
    select
        user_id,
        min(created_at_ts) as first_order_ts,
        max(created_at_ts) as last_order_ts,
        count(distinct order_id) as total_orders,
        sum(sale_price) as lifetime_revenue, -- นี่แหละคือตัวแปรหลักของ CLV
        avg(sale_price) as average_order_value
    from order_items
    where item_status not in ('Cancelled', 'Returned') -- คิดเฉพาะยอดที่ขายได้จริง
    group by 1
),

final as (
    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.age,
        u.country,
        v.first_order_ts,
        v.last_order_ts,
        coalesce(v.total_orders, 0) as total_orders,
        coalesce(v.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(v.average_order_value, 0) as average_order_value,
        
        -- Business Logic: แบ่งเกรดลูกค้า (Customer Segmentation)
        case 
            when v.lifetime_revenue > 500 then 'High Value'
            when v.lifetime_revenue between 100 and 500 then 'Medium Value'
            when v.lifetime_revenue > 0 then 'Low Value'
            else 'Lead' 
        end as customer_segment
    from users u
    left join user_value v on u.user_id = v.user_id
)

select * from final
