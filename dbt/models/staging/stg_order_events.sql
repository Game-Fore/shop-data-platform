-- Все версии заказов из журнала CDC: одна строка = одно событие (создание, смена суммы
-- или статуса, удаление). FINAL здесь не про "последнюю версию", а про дедупликацию
-- повторных доставок: ключ сортировки журнала (order_id, _lsn) уникален для события.
select
    order_id,
    customer_id,
    status,
    total as order_total,
    created_at,
    updated_at as event_at,  -- бизнес-время события (в Postgres updated_at меняется на каждый UPDATE)
    _op as op,
    _is_deleted = 1 as is_delete,
    _lsn as lsn
from {{ source('shop', 'orders_changelog') }} final
