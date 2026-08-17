{#-
  Forces an immediate refresh of a ClickHouse Refreshable Materialized View,
  outside its own REFRESH EVERY schedule. Needed because dbt-clickhouse names
  the actual MV object "<model_name>_mv" (the model name itself is the
  queryable target table) — pass the base model name, not the _mv suffix.

  Usage: dbt run-operation refresh_materialized_view --args '{view_name: gold.daily_ride_summary}'
-#}
{% macro refresh_materialized_view(view_name) %}
  {% set mv_name = view_name ~ '_mv' %}
  {% set query %}
    SYSTEM REFRESH VIEW {{ mv_name }}
  {% endset %}
  {% do run_query(query) %}
  {{ log('Refreshed materialized view: ' ~ mv_name, info=True) }}
{% endmacro %}
