{# Сегодняшняя дата в бизнес-таймзоне: граница "полного" периода в витринах. #}
{% macro business_today() -%}
    toDate(now(), '{{ var("business_tz") }}')
{%- endmacro %}
