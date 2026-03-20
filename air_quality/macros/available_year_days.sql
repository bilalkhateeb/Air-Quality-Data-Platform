{% macro available_year_days(target_date, min_available_date, max_available_date) %}
    datediff(
        least(
            cast(dateadd(day, -1, dateadd(year, 1, date_trunc('year', {{ target_date }}))) as date),
            cast({{ max_available_date }} as date)
        ),
        greatest(
            cast(date_trunc('year', {{ target_date }}) as date),
            cast({{ min_available_date }} as date)
        )
    ) + 1
{% endmacro %}