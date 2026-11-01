-- Сумма заказа должна совпасть с суммой его позиций.
-- Заказы с только что пришедшими событиями пропускаются (см. макрос settling_order_ids).
select order_id, order_total, items_total
from {{ ref('fct_orders') }}
where not is_deleted
  and order_id not in {{ settling_order_ids() }}
  and (items_total is null or abs(order_total - items_total) > 0.01)
