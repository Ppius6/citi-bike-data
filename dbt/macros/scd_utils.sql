{% macro is_current_flag(valid_to_col) %}
    case when {{ valid_to_col }} is null then 1 else 0 end
{% endmacro %}