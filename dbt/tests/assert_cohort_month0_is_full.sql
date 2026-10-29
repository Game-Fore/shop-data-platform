-- В месяц когорты (month_offset = 0) активны все её клиенты - по определению когорты.
-- Если нет, определения когорты в dim_customers и активности в витрине разошлись.
select cohort_month, cohort_size, active_customers
from {{ ref('mart_cohort_retention') }}
where month_offset = 0 and active_customers != cohort_size
