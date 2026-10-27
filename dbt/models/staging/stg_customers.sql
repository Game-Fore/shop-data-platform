-- Текущее состояние клиентов. FINAL схлопывает версии по _lsn и скрывает строки с _is_deleted = 1;
-- явный фильтр оставлен, чтобы модель не зависела от настроек сервера.
select
    id as customer_id,
    email,
    full_name,
    city,
    created_at,
    toDate(created_at, '{{ var("business_tz") }}') as signup_date,
    _ingested_at
from {{ source('shop', 'customers') }} final
where _is_deleted = 0
