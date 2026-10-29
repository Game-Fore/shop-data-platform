-- Сумма заказа должна совпасть с суммой его позиций.
-- Свежие заказы исключены: orders и order_items приходят в ClickHouse разными INSERT
-- (и могут попасть в разные батчи consumer'а), поэтому сразу после записи возможна
-- короткая рассинхронизация. Это eventual consistency пайплайна, а не ошибка данных.
select order_id, order_total, items_total
from {{ ref('fct_orders') }}
where not is_deleted
  and created_at < now() - interval 5 minute
  and (items_total is null or abs(order_total - items_total) > 0.01)
