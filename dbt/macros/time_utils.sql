{% macro convert_to_eastern(column) %}
    {{ column }} AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York'
{% endmacro %}

{% macro day_name(col) %}
    multiIf(
        toDayOfWeek({{ col }}) = 1, 'Monday',
        toDayOfWeek({{ col }}) = 2, 'Tuesday',
        toDayOfWeek({{ col }}) = 3, 'Wednesday',
        toDayOfWeek({{ col }}) = 4, 'Thursday',
        toDayOfWeek({{ col }}) = 5, 'Friday',
        toDayOfWeek({{ col }}) = 6, 'Saturday',
        'Sunday'
        )   
{% endmacro %}

{% macro month_name(col) %}
    multiIf(
        toMonth({{ col }}) = 1, 'January',
        toMonth({{ col }}) = 2, 'February',
        toMonth({{ col }}) = 3, 'March',
        toMonth({{ col }}) = 4, 'April',
        toMonth({{ col }}) = 5, 'May',
        toMonth({{ col }}) = 6, 'June',
        toMonth({{ col }}) = 7, 'July',
        toMonth({{ col }}) = 8, 'August',
        toMonth({{ col }}) = 9, 'September',
        toMonth({{ col }}) = 10, 'October',
        toMonth({{ col }}) = 11, 'November',
        'December'
    )
{% endmacro %}

{% macro get_season(col) %}
    multiIf(
        toMonth({{ col }}) IN (12, 1, 2), 'Winter',
        toMonth({{ col }}) IN (3, 4, 5), 'Spring',
        toMonth({{ col }}) IN (6, 7, 8), 'Summer',
        'Fall'
    )
{% endmacro %}
