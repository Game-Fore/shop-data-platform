-- Ключ сортировки - из колонок самой позиции: атрибуты заказа пришли через LEFT JOIN
-- и Nullable (join_use_nulls), а Nullable в ключе MergeTree запрещён по умолчанию.
{{ config(order_by='(order_id, order_item_id)') }}

-- Позиции заказов, денормализованные атрибутами заказа и товара: для анализа по категориям
-- не нужно джойнить три таблицы. LEFT JOIN, а не INNER: позиция без заказа - это ошибка
-- данных, её должен поймать тест relationships, а не тихо отбросить join.
select
    i.order_item_id as order_item_id,
    i.order_id as order_id,
    o.customer_id as customer_id,
    i.product_id as product_id,
    p.product_name as product_name,
    p.category as category,
    i.quantity as quantity,
    i.unit_price as unit_price,
    i.line_total as line_total,
    o.order_date as order_date,
    o.status as status,
    o.is_revenue as is_revenue
from {{ ref('stg_order_items') }} as i
left join {{ ref('fct_orders') }} as o on o.order_id = i.order_id
left join {{ ref('stg_products') }} as p on p.product_id = i.product_id
