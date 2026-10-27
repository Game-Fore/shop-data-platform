{#
  По умолчанию dbt склеивает имена: <target.schema>_<custom schema> -> analytics_staging.
  Для одного окружения это лишний шум, поэтому база = custom schema как есть
  (staging, intermediate, marts), а без custom schema - target.schema (analytics).
  Если появится отдельное prod-окружение в том же ClickHouse, здесь стоит вернуть префикс.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
