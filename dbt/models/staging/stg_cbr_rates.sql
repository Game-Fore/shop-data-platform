-- Официальный курс ЦБ: рублей за единицу валюты. Только дни, когда ЦБ его публиковал.
select
    rate_date,
    currency_code,
    rub_per_unit
from {{ source('external', 'cbr_rates') }} final
