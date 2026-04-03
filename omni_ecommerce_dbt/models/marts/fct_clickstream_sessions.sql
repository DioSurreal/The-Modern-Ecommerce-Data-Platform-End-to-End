{{ config(materialized='table') }}

with sessionized_events as (
    select * from {{ ref('int_sessionized_events') }}
),

customers as (
    select * from {{ ref('stg_users') }} -- จากโปรเจกต์ F2 เดิมของมึง
)

select
    e.event_id,
    e.enhanced_session_id,
    e.event_ts,
    e.event_action,
    e.page_url,
    e.browser,
    c.user_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.email
from sessionized_events e
left join customers c on e.user_id = cast(c.user_id as string) -- Join ถ้ามี user_id