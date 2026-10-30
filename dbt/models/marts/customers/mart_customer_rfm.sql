{{ config(order_by='customer_id') }}

-- RFM-сегментация покупателей (клиенты хотя бы с одной покупкой).
-- Правила выбраны в notebooks/04_rfm.ipynb; Python-версия той же логики - analytics/rfm.py,
-- ноутбук сверяет, что сегменты совпадают.
--   R, M - квинтили (quantilesExactInclusive = линейная интерполяция, как np.quantile);
--   F    - фиксированные пороги 1 | 2 | 3-4 | 5-7 | 8+: половина клиентов купила один раз,
--          и квинтили F вырождаются в [1, 1, 2, 3].
-- Давность считается от даты среза (последний заказ в данных), а не от сегодня: сегменты
-- не зависят от дня, когда собрана витрина, даже если поток остановился.
with buyers as (
    -- фильтр во внутреннем запросе: снаружи алиасы перекрыли бы колонки в WHERE (см. README)
    select
        customer_id,
        city,
        cohort_month,
        assumeNotNull(days_since_last_purchase) as recency_days,
        purchases_count as frequency,
        lifetime_revenue as monetary
    from (
        select * from {{ ref('dim_customers') }} where purchases_count > 0
    )
),

bounds as (
    select
        quantilesExactInclusive(0.2, 0.4, 0.6, 0.8)(toFloat64(recency_days)) as r_bounds,
        quantilesExactInclusive(0.2, 0.4, 0.6, 0.8)(toFloat64(monetary)) as m_bounds
    from buyers
),

scored as (
    select
        b.customer_id as customer_id,
        b.city as city,
        b.cohort_month as cohort_month,
        b.recency_days as recency_days,
        b.frequency as frequency,
        b.monetary as monetary,
        -- значение на границе уходит в нижнюю группу (строгое >), как в analytics/rfm.py
        toUInt8(5 - arrayCount(x -> toFloat64(b.recency_days) > x, q.r_bounds)) as r_score,
        toUInt8(1 + arrayCount(t -> b.frequency >= t, [2, 3, 5, 8])) as f_score,
        toUInt8(1 + arrayCount(x -> toFloat64(b.monetary) > x, q.m_bounds)) as m_score
    from buyers as b
    cross join bounds as q
)

select
    *,
    concat(toString(r_score), toString(f_score), toString(m_score)) as rfm_code,
    -- порядок правил важен: первое подходящее задаёт сегмент
    multiIf(
        r_score >= 4 and f_score >= 4, 'Чемпионы',
        r_score >= 3 and f_score >= 3, 'Лояльные',
        r_score >= 4 and f_score <= 2, 'Новички',
        r_score <= 2 and f_score >= 3, 'Под угрозой',
        r_score = 3 and f_score <= 2, 'Требуют внимания',
        r_score = 2 and f_score <= 2, 'Засыпают',
        'Потерянные'
    ) as segment
from scored
