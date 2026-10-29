{{ config(order_by='product_id') }}

-- Товар + продажи за всю историю (только позиции заказов с выручкой).
-- revenue считается по цене на момент заказа, current_price - справочно.
with sales as (
    select
        product_id,
        sumIf(quantity, is_revenue) as units_sold,
        sumIf(line_total, is_revenue) as revenue,
        uniqExactIf(order_id, is_revenue) as orders_count,
        minIfOrNull(order_date, is_revenue) as first_sold_date,
        maxIfOrNull(order_date, is_revenue) as last_sold_date
    from {{ ref('fct_order_items') }}
    group by product_id
)

select
    p.product_id as product_id,
    p.product_name as product_name,
    p.category as category,
    p.current_price as current_price,
    coalesce(s.units_sold, 0) as units_sold,
    coalesce(s.revenue, 0) as revenue,
    coalesce(s.orders_count, 0) as orders_count,
    s.first_sold_date as first_sold_date,
    s.last_sold_date as last_sold_date
from {{ ref('stg_products') }} as p
left join sales as s on s.product_id = p.product_id
