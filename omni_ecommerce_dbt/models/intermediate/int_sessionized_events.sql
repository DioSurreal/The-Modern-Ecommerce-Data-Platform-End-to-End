{{ config(materialized='view') }}

with events as (
    select * from {{ ref('stg_clickstream') }}
),

time_diffs as (
    select 
        *,
        lag(event_ts) over(partition by raw_session_id order by event_ts) as prev_event_ts
    from events
),

session_flags as (
    select 
        *,
        case 
            when prev_event_ts is null then 1 
            when timediff(second, prev_event_ts, event_ts) > 1800 then 1 
            else 0 
        end as is_new_session_flag
    from time_diffs
),

final as (
    select
        *,
        raw_session_id || '-' || sum(is_new_session_flag) over(partition by raw_session_id order by event_ts rows between unbounded preceding and current row) as enhanced_session_id
    from session_flags
)

select * from final