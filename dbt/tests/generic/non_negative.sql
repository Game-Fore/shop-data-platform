{# Свой generic-тест: значения колонки не отрицательные. NULL не считается ошибкой. #}
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
