select
    id as product_id,
    name as product_name,
    category,
    price as current_price
from {{ source('shop', 'products') }} final
where _is_deleted = 0
