select
    experiment_id,
    customer_id,
    variant,
    assigned_at
from {{ source('shop', 'experiment_assignments') }} final
where _is_deleted = 0
