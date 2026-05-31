{% macro clean_string(column) %}
    NULLIF(TRIM({{ column }}), '')
{% endmacro %}