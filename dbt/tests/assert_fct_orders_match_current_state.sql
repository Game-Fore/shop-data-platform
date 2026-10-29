-- fct_orders собирается из журнала CDC, stg_orders - из текущего состояния (FINAL).
-- Для не удалённых заказов оба пути должны дать одно и то же: это сверка MV-журнала
-- с основной таблицей внутри dbt (аналог проверки orders_changelog в verify.py).
select
    coalesce(f.order_id, s.order_id) as order_id,
    f.status as fct_status,
    s.status as stg_status,
    f.order_total as fct_total,
    s.order_total as stg_total
from (select * from {{ ref('fct_orders') }} where not is_deleted) as f
full outer join {{ ref('stg_orders') }} as s on s.order_id = f.order_id
where f.order_id is null
   or s.order_id is null
   or f.status != s.status
   or f.order_total != s.order_total
