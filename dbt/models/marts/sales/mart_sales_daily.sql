{{ config(order_by='date') }}

-- Продажи по дням (по дате создания заказа в бизнес-таймзоне).
-- Сетка дат строится отдельно: день без заказов должен быть строкой с нулями, а не дырой,
-- иначе графики "соединят" соседние точки и средние по дням будут завышены.
--
-- Важно: метрики последних дней ещё "дозревают" - заказ, созданный вчера, может оплатиться
-- или отмениться сегодня. Поэтому витрина пересчитывается целиком, а не дописывается.
with orders as (
    select * from {{ ref('fct_orders') }}
),

bounds as (
    select
        min(order_date) as first_date,
        -- дата среза: сетка заканчивается на последнем заказе, а не на сегодняшнем дне, иначе
        -- при остановленном потоке появились бы хвосты нулей, похожие на обвал продаж
        max(order_date) as last_date
    from orders
),

spine as (
    select addDays(first_date, arrayJoin(range(toUInt32(dateDiff('day', first_date, last_date) + 1)))) as date
    from bounds
),

daily as (
    select
        order_date as date,
        count() as orders,
        countIf(is_revenue) as paid_orders,
        countIf(status = 'cancelled') as cancelled_orders,
        sum(order_total) as gmv,
        sumIf(order_total, is_revenue) as revenue,
        uniqExactIf(customer_id, is_revenue) as buyers,
        sumIf(units, is_revenue) as units_sold
    from orders
    group by order_date
),

new_buyers as (
    select first_purchase_date as date, count() as new_buyers
    from {{ ref('dim_customers') }}
    where first_purchase_date is not null
    group by first_purchase_date
)

select
    s.date as date,
    toDayOfWeek(s.date) as day_of_week,  -- 1 = понедельник
    coalesce(d.orders, 0) as orders,
    coalesce(d.paid_orders, 0) as paid_orders,
    coalesce(d.cancelled_orders, 0) as cancelled_orders,
    -- GMV: все оформленные заказы; revenue: только оплаченные (var revenue_statuses)
    coalesce(d.gmv, 0) as gmv,
    coalesce(d.revenue, 0) as revenue,
    coalesce(d.buyers, 0) as buyers,
    coalesce(n.new_buyers, 0) as new_buyers,
    coalesce(d.units_sold, 0) as units_sold,
    -- средний чек и доля отмен: NULL в дни без заказов (деление на 0 - не ноль, а "не определено")
    d.revenue / nullIf(d.paid_orders, 0) as avg_order_value,
    d.cancelled_orders / nullIf(d.orders, 0) as cancel_rate,
    -- рубли по курсу ЦБ на дату заказа: фактическая выручка на момент продажи, колебания
    -- курса после неё не переписывают прошлое
    x.usd_rub_rate as usd_rub_rate,
    round(coalesce(d.gmv, 0) * x.usd_rub_rate, 2) as gmv_rub,
    round(coalesce(d.revenue, 0) * x.usd_rub_rate, 2) as revenue_rub,
    round(d.revenue / nullIf(d.paid_orders, 0) * x.usd_rub_rate, 2) as avg_order_value_rub,
    -- день среза обычно неполный: история заканчивается посреди дня
    s.date < b.last_date as is_complete_period,
    -- неделя (пн-вс) целиком внутри данных: неполные недели на краях выглядели бы на недельных
    -- графиках как обвал. День среза обычно неполный, поэтому строгое <
    toMonday(s.date) >= b.first_date and addDays(toMonday(s.date), 6) < b.last_date as is_full_week
from spine as s
cross join bounds as b
left join daily as d on d.date = s.date
left join {{ ref('dim_exchange_rates') }} as x on x.date = s.date
left join new_buyers as n on n.date = s.date
