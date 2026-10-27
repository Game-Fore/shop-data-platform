select
    id as order_id,
    customer_id,
    status,
    {{ is_revenue_status('status') }} as is_revenue,
    total as order_total,
    created_at,
    updated_at,
    toDate(created_at, '{{ var("business_tz") }}') as order_date,
    _ingested_at
from {{ source('shop', 'orders') }} final
where _is_deleted = 0
