{{ config(order_by='date') }}

-- Курс доллара на каждый день истории заказов. ЦБ не публикует курс на воскресенье, понедельник
-- и праздники: в эти дни действует последний опубликованный, его находит ASOF JOIN.
-- Столбец k нужен потому, что ASOF JOIN в ClickHouse требует хотя бы одно условие равенства.
with bounds as (
    select min(order_date) as first_date, {{ data_cutoff_date() }} as last_date
    from {{ ref('fct_orders') }}
),

spine as (
    select
        1 as k,
        addDays(first_date, arrayJoin(range(toUInt32(dateDiff('day', first_date, last_date) + 1)))) as date
    from bounds
),

rates as (
    select 1 as k, rate_date, rub_per_unit
    from {{ ref('stg_cbr_rates') }}
    where currency_code = 'USD'
)

select
    s.date as date,
    r.rub_per_unit as usd_rub_rate,
    r.rate_date as rate_date,
    -- 0 = курс опубликован на этот день; большие значения означают, что загрузка отстала
    dateDiff('day', r.rate_date, s.date) as rate_age_days
from spine as s
asof left join rates as r on s.k = r.k and s.date >= r.rate_date
