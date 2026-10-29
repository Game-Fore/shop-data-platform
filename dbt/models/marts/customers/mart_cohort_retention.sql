{{ config(order_by='(cohort_month, month_offset)') }}

-- Месячный retention по когортам.
-- Когорта = месяц первой покупки; клиент "активен" в месяце, если сделал в нём покупку.
--
-- Сетка cohort x month_offset строится только до текущего месяца включительно:
--   * наблюдаемый месяц без покупок = строка с 0 (клиенты действительно не вернулись);
--   * ненаблюдаемый (ещё не наступил) = строки нет, в pivot это NULL.
-- Если ненаблюдаемые ячейки заполнить нулями, средний retention по когортам будет занижен:
-- молодые когорты "потянут вниз" поздние месяцы, которых они просто не прожили.
with purchases as (
    select
        customer_id,
        toStartOfMonth(order_date) as activity_month,
        sum(order_total) as revenue
    from {{ ref('fct_orders') }}
    where is_revenue
    group by customer_id, activity_month
),

customers as (
    -- cohort_month в dim_customers Nullable (LEFT JOIN); assumeNotNull снимает Nullable с типа,
    -- чтобы колонку можно было взять в ключ сортировки.
    -- Фильтр - строго во внутреннем запросе: в ClickHouse алиас из SELECT виден в WHERE и
    -- перекрывает колонку, и "where cohort_month is not null" на этом уровне проверял бы
    -- assumeNotNull(cohort_month), который для NULL возвращает 1970-01-01, а не NULL.
    select customer_id, assumeNotNull(cohort_month) as cohort_month
    from (
        select customer_id, cohort_month
        from {{ ref('dim_customers') }}
        where cohort_month is not null
    )
),

cohorts as (
    select cohort_month, count() as cohort_size
    from customers
    group by cohort_month
),

grid as (
    select
        cohort_month,
        cohort_size,
        arrayJoin(range(toUInt32(
            dateDiff('month', cohort_month, toStartOfMonth({{ business_today() }})) + 1
        ))) as month_offset
    from cohorts
),

activity as (
    select
        c.cohort_month as cohort_month,
        toUInt32(dateDiff('month', c.cohort_month, p.activity_month)) as month_offset,
        uniqExact(p.customer_id) as active_customers,
        sum(p.revenue) as revenue
    from purchases as p
    inner join customers as c on c.customer_id = p.customer_id
    group by cohort_month, month_offset
)

select
    g.cohort_month as cohort_month,
    g.month_offset as month_offset,
    addMonths(g.cohort_month, g.month_offset) as activity_month,
    g.cohort_size as cohort_size,
    coalesce(a.active_customers, 0) as active_customers,
    coalesce(a.active_customers, 0) / g.cohort_size as retention_rate,
    coalesce(a.revenue, 0) as revenue,
    -- текущий месяц ещё идёт: его retention вырастет к концу месяца
    addMonths(g.cohort_month, g.month_offset) < toStartOfMonth({{ business_today() }}) as is_complete_period
from grid as g
left join activity as a
    on a.cohort_month = g.cohort_month and a.month_offset = g.month_offset
