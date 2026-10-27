{# Условие "заказ приносит выручку". Список статусов один на проект: var('revenue_statuses'). #}
{% macro is_revenue_status(column) -%}
    {#- cast: для LowCardinality(String) результат in() был бы LowCardinality(UInt8), а такие колонки ClickHouse запрещает -#}
    cast({{ column }} in ({% for s in var('revenue_statuses') %}'{{ s }}'{% if not loop.last %}, {% endif %}{% endfor %}) as Bool)
{%- endmacro %}
