-- Суммы в витрине по дням должны сходиться с фактом: ни один заказ не потерян
-- и не задвоен при построении сетки дат и джойнах.
select m.orders, f.orders, m.revenue, f.revenue
from (select sum(orders) as orders, sum(revenue) as revenue from {{ ref('mart_sales_daily') }}) as m
cross join (select count() as orders, sumIf(order_total, is_revenue) as revenue from {{ ref('fct_orders') }}) as f
where m.orders != f.orders or m.revenue != f.revenue
