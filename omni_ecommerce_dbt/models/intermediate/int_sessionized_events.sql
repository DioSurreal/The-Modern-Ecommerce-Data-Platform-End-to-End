{{ config(materialized='view') }}

with events as (
    select * from {{ ref('stg_clickstream') }}
),

time_diffs as (
    select 
        *,
        -- ใช้ raw_session_id เป็นตัวแบ่งกลุ่มคน (แทน user_id ที่หายไป)
        lag(event_ts) over(partition by raw_session_id order by event_ts) as prev_event_ts
    from events
),

session_flags as (
    select 
        *,
        case 
            when prev_event_ts is null then 1 -- เริ่มต้น event แรกเป็น session ใหม่เสมอ
            -- 30 นาที = 1800 วินาที
            when timediff(second, prev_event_ts, event_ts) > 1800 then 1 
            else 0 
        end as is_new_session_flag
    from time_diffs
),

final as (
    select
        *,
        -- สร้าง Unique Session ID โดยการรวม raw_session_id กับลำดับที่เพิ่มขึ้น
        raw_session_id || '-' || sum(is_new_session_flag) over(partition by raw_session_id order by event_ts rows between unbounded preceding and current row) as enhanced_session_id
    from session_flags
)

select * from final