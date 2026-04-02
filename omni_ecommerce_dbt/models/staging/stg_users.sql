{{ config(materialized='view') }} -- Staging มักจะเป็น View เพื่อประหยัดพื้นที่และสดใหม่เสมอ

with source as (
    select * from {{ source('ecommerce_raw', 'users') }}
),

renamed as (
    select
        -- Primary Key
        id as user_id,
        
        -- User Details (ทำความสะอาด String เล็กน้อย)
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        email,
        age,
        gender,
        
        -- Location Data
        street_address,
        city,
        state,
        country,
        postal_code,
        latitude,
        longitude,
        
        -- Marketing & Metadata
        traffic_source,
        -- แปลง String เป็น Timestamp (Snowflake Syntax)
        cast(created_at as timestamp_ntz) as created_at_ts

    from source
)

select * from renamed