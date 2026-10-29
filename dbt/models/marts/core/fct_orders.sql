{{ config(order_by='(order_date, order_id)') }}

-- Одна строка = один заказ за всю историю, включая удалённые в источнике.
-- Строится из журнала CDC, а не из stg_orders: удалённый отменённый заказ остался только
-- в журнале, а без него занижались бы отмены в воронке и cancel rate.
with versions as (
    select
        order_id,
        argMax(customer_id, lsn) as customer_id,
        -- delete-событие несёт образ строки до удаления, но берём явно последний не-delete
        argMaxIf(status, lsn, not is_delete) as status,
        argMaxIf(order_total, lsn, not is_delete) as order_total,
        any(created_at) as created_at,
        max(is_delete) as is_deleted
    from {{ ref('stg_order_events') }}
    group by order_id
),

status_times as (
    -- *OrNull: без него ClickHouse для "не было такого статуса" вернул бы 1970-01-01
    select
        order_id,
        minIfOrNull(status_at, status = 'paid') as paid_at,
        minIfOrNull(status_at, status = 'shipped') as shipped_at,
        minIfOrNull(status_at, status = 'delivered') as delivered_at,
        minIfOrNull(status_at, status = 'cancelled') as cancelled_at
    from {{ ref('int_order_status_events') }}
    group by order_id
),

items as (
    select
        order_id,
        count() as items_count,
        sum(quantity) as units,
        sum(line_total) as items_total
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    v.order_id as order_id,
    v.customer_id as customer_id,
    v.status as status,
    {{ is_revenue_status('v.status') }} as is_revenue,
    v.is_deleted as is_deleted,
    v.order_total as order_total,
    -- у удалённых заказов позиции удалены каскадом, поэтому здесь NULL
    i.items_count as items_count,
    i.units as units,
    i.items_total as items_total,
    v.created_at as created_at,
    toDate(v.created_at, '{{ var("business_tz") }}') as order_date,
    s.paid_at as paid_at,
    s.shipped_at as shipped_at,
    s.delivered_at as delivered_at,
    s.cancelled_at as cancelled_at,
    dateDiff('second', v.created_at, s.paid_at) / 3600 as hours_to_paid,
    dateDiff('second', s.paid_at, s.shipped_at) / 3600 as hours_paid_to_shipped,
    dateDiff('second', s.shipped_at, s.delivered_at) / 3600 as hours_shipped_to_delivered,
    -- порядковый номер среди всех заказов клиента (1 = первый заказ, в т.ч. отменённый)
    row_number() over (partition by v.customer_id order by v.created_at, v.order_id) as customer_order_number
from versions as v
left join status_times as s on s.order_id = v.order_id
left join items as i on i.order_id = v.order_id
