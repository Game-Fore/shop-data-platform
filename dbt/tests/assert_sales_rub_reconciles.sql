-- Рублёвая выручка по дням сходится с пересчётом каждого заказа по курсу на его дату.
-- Допуск 1 рубль на весь период: витрина округляет дневные суммы до копеек.
select m.revenue_rub, o.revenue_rub
from (select sum(revenue_rub) as revenue_rub from {{ ref('mart_sales_daily') }}) as m
cross join (
    select sum(f.order_total * r.usd_rub_rate) as revenue_rub
    from {{ ref('fct_orders') }} as f
    inner join {{ ref('dim_exchange_rates') }} as r on r.date = f.order_date
    where f.is_revenue
) as o
where abs(toFloat64(m.revenue_rub) - toFloat64(o.revenue_rub)) > 1
