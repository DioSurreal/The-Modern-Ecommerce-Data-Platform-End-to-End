
with source_data as (
    select 1 as id
    union all
    select 2 as id     -- <--- เปลี่ยนจาก null เป็นเลขอื่นซะ (เช่น 2)
)


select *
from source_data
where id is not null
