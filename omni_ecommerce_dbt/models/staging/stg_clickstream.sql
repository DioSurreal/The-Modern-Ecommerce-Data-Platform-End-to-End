
-- แก้บรรทัดนี้
with raw_data as (
    select * from {{ source('streaming', 'raw_clickstream') }} 
),

final as (
    select
        event_data:id::integer as event_id,
        coalesce(event_data:user_id::string, 'anonymous') as user_id,
        event_data:session_id::string as raw_session_id,
        event_data:created_at::timestamp_ntz as event_ts,
        event_data:event_type::string as event_action,
        event_data:uri::string as page_url,
        event_data:browser::string as browser,
        metadata_filename,
        ingested_at
    from raw_data
)

select * from final
