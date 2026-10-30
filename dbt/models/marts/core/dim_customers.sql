{{ config(order_by='customer_id') }}

-- Клиент + агрегаты по заказам: основа для RFM и когорт.
-- "Покупка" = заказ со статусом из var revenue_statuses; "заказ" = любой, включая отменённые.
-- email и ФИО в витрину не выносим: аналитике они не нужны, а BI-доступ к персональным
-- данным - лишний риск. При необходимости они есть в staging.stg_customers.
with orders as (
    select
        customer_id,
        count() as orders_count,
        countIf(is_revenue) as purchases_count,
        sumIf(order_total, is_revenue) as lifetime_revenue,
        min(created_at) as first_order_at,
        max(created_at) as last_order_at,
        minIfOrNull(order_date, is_revenue) as first_purchase_date,
        maxIfOrNull(order_date, is_revenue) as last_purchase_date
    from {{ ref('fct_orders') }}
    group by customer_id
)

select
    c.customer_id as customer_id,
    c.city as city,
    c.created_at as created_at,
    c.signup_date as signup_date,
    coalesce(o.orders_count, 0) as orders_count,
    coalesce(o.purchases_count, 0) as purchases_count,
    coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
    o.first_order_at as first_order_at,
    o.last_order_at as last_order_at,
    o.first_purchase_date as first_purchase_date,
    o.last_purchase_date as last_purchase_date,
    -- когорта = месяц первой покупки; у клиентов без покупок NULL
    toStartOfMonth(o.first_purchase_date) as cohort_month,
    dateDiff('day', o.last_purchase_date, {{ data_cutoff_date() }}) as days_since_last_purchase
from {{ ref('stg_customers') }} as c
left join orders as o on o.customer_id = c.customer_id
