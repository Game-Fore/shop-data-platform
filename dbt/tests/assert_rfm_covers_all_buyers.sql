-- Каждый покупатель из dim_customers попал в RFM-витрину ровно один раз (и никто лишний).
select b.buyers, r.rfm_rows
from (select count() as buyers from {{ ref('dim_customers') }} where purchases_count > 0) as b
cross join (select count() as rfm_rows from {{ ref('mart_customer_rfm') }}) as r
where b.buyers != r.rfm_rows
