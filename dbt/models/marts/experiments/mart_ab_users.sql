{{ config(order_by='(experiment_id, customer_id)') }}

-- Одна строка = один участник эксперимента: группа и его метрики
--   * в окне эксперимента: от назначения до конца эксперимента (новые клиенты входят
--     в эксперимент при регистрации, поэтому окно у них короче);
--   * в предпериоде той же длины до старта: для A/A-проверки и CUPED.
-- Анализ - intention-to-treat: в эксперименте все назначенные, включая тех, кто ничего
-- не купил. Иначе "покупатели" в двух группах были бы несравнимы (скидка меняет состав
-- покупателей), и рандомизация потеряла бы смысл.
with experiments as (
    select
        e.experiment_id as experiment_id,
        e.duration_days as duration_days,
        s.window_start as window_start,
        s.window_start + toIntervalDay(e.duration_days) as window_end,
        s.window_start - toIntervalDay(e.duration_days) as pre_start
    from {{ ref('experiments') }} as e
    inner join (
        select experiment_id, min(assigned_at) as window_start
        from {{ ref('stg_experiment_assignments') }}
        group by experiment_id
    ) as s on s.experiment_id = e.experiment_id
),

users as (
    select
        a.experiment_id as experiment_id,
        a.customer_id as customer_id,
        a.variant as variant,
        a.assigned_at as assigned_at,
        x.window_start as window_start,
        x.window_end as window_end,
        x.pre_start as pre_start
    from {{ ref('stg_experiment_assignments') }} as a
    inner join experiments as x on x.experiment_id = a.experiment_id
),

user_orders as (
    -- флаги периода считаются здесь, а агрегируются ниже: так условия не дублируются
    -- в каждом countIf/sumIf. Для клиентов без заказов (join_use_nulls) флаги = NULL -> не считаются
    select
        u.experiment_id as experiment_id,
        u.customer_id as customer_id,
        o.created_at >= u.assigned_at and o.created_at < u.window_end as in_window,
        o.created_at >= u.pre_start and o.created_at < u.window_start as in_pre,
        o.is_revenue as is_revenue,
        o.status as status,
        o.order_total as order_total
    from users as u
    left join {{ ref('fct_orders') }} as o on o.customer_id = u.customer_id
),

metrics as (
    select
        experiment_id,
        customer_id,
        countIf(in_window) as orders,
        countIf(in_window and is_revenue) as purchases,
        countIf(in_window and status = 'cancelled') as cancelled_orders,
        sumIf(order_total, in_window and is_revenue) as revenue,
        countIf(in_pre and is_revenue) as pre_purchases,
        sumIf(order_total, in_pre and is_revenue) as pre_revenue
    from user_orders
    group by experiment_id, customer_id
)

select
    u.experiment_id as experiment_id,
    u.customer_id as customer_id,
    u.variant as variant,
    u.assigned_at as assigned_at,
    u.window_start as window_start,
    u.window_end as window_end,
    -- клиент пришёл уже во время эксперимента: предпериода у него нет (pre_* = 0)
    u.assigned_at > u.window_start as is_new_customer,
    coalesce(m.orders, 0) as orders,
    coalesce(m.purchases, 0) as purchases,
    coalesce(m.purchases, 0) > 0 as converted,
    coalesce(m.cancelled_orders, 0) as cancelled_orders,
    coalesce(m.revenue, 0) as revenue,
    coalesce(m.pre_purchases, 0) as pre_purchases,
    coalesce(m.pre_revenue, 0) as pre_revenue
from users as u
left join metrics as m
    on m.experiment_id = u.experiment_id and m.customer_id = u.customer_id
