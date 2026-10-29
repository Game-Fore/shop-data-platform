-- В витрине по дням нет пропущенных дат: число строк = длина интервала в днях.
select count() as days, dateDiff('day', min(date), max(date)) + 1 as expected_days
from {{ ref('mart_sales_daily') }}
having days != expected_days
