select
    user_id,
    lifetime_revenue
from {{ ref('int_customer_orders') }}
where lifetime_revenue < 0