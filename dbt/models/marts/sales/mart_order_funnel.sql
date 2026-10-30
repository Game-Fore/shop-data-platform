{{ config(order_by='(order_date, stage_order)') }}

-- Воронка заказа по дням для BI: сколько заказов дошло до каждого этапа.
-- is_matured отделяет заказы, у которых было время дойти до конца (правое цензурирование,
-- notebooks/02_funnel.ipynb): иначе свежие заказы занижают конверсию в доставку.
-- Возраст считается от последнего заказа в данных, а не от now(): если поток остановился,
-- статусы свежих заказов застыли, и с течением времени они не становятся "созревшими".
with orders as (
    select
        order_date,
        paid_at,
        shipped_at,
        delivered_at,
        created_at < (select max(created_at) from {{ ref('fct_orders') }})
            - toIntervalDay({{ var("funnel_maturity_days") }}) as is_matured
    from {{ ref('fct_orders') }}
),

stages as (
    select order_date, is_matured, 'Создан' as stage, 1 as stage_order from orders
    union all
    select order_date, is_matured, 'Оплачен', 2 from orders where paid_at is not null
    union all
    select order_date, is_matured, 'Отгружен', 3 from orders where shipped_at is not null
    union all
    select order_date, is_matured, 'Доставлен', 4 from orders where delivered_at is not null
)

select
    order_date,
    is_matured,
    stage,
    toUInt8(stage_order) as stage_order,
    count() as orders,
    -- сколько заказов создано в тот же день: знаменатель конверсии, чтобы BI считал
    -- sum(orders) / sum(created_orders) для любого периода без подзапросов
    max(count()) over (partition by order_date, is_matured) as created_orders
from stages
group by order_date, is_matured, stage, stage_order
