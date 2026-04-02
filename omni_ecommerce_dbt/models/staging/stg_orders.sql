with source as (
    select * from {{ source('ecommerce_raw', 'orders') }}
),

renamed as (
    select
        order_id,
        user_id,
        status,
        gender,
        num_of_item,
        -- จัดการวันที่ให้เป็น Timestamp
        cast(created_at as timestamp_ntz) as created_at_ts,
        cast(shipped_at as timestamp_ntz) as shipped_at_ts,
        cast(delivered_at as timestamp_ntz) as delivered_at_ts,
        cast(returned_at as timestamp_ntz) as returned_at_ts
    from source
)

select * from renamed