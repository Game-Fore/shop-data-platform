-- Воронка не может расширяться: на каждом следующем этапе заказов не больше, чем на предыдущем.
-- А на первом этапе их ровно столько, сколько всего заказов в fct_orders.
with totals as (
    select
        sumIf(orders, stage_order = 1) as created,
        sumIf(orders, stage_order = 2) as paid,
        sumIf(orders, stage_order = 3) as shipped,
        sumIf(orders, stage_order = 4) as delivered
    from {{ ref('mart_order_funnel') }}
)

select t.created, t.paid, t.shipped, t.delivered, f.total_orders
from totals as t
cross join (select count() as total_orders from {{ ref('fct_orders') }}) as f
where t.created != f.total_orders
   or t.paid > t.created
   or t.shipped > t.paid
   or t.delivered > t.shipped
