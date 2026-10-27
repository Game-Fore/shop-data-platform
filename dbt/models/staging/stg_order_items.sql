select
    id as order_item_id,
    order_id,
    product_id,
    quantity,
    price as unit_price,
    price * quantity as line_total
from {{ source('shop', 'order_items') }} final
where _is_deleted = 0
