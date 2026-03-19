{% macro dremio__date_spine(datepart, start_date, end_date) %}

    {% if datepart != 'hour' %}
        {% do exceptions.raise_compiler_error("This override currently supports only datepart='hour'") %}
    {% endif %}

    with rawdata as (

        {{ dbt_utils.generate_series(upper_bound=100000) }}

    ),

    filtered_numbers as (

        select
            generated_number
        from rawdata
        where generated_number <= (
            select timestampdiff(SQL_TSI_HOUR, {{ start_date }}, {{ end_date }})
        )

    ),

    all_periods as (

        select
            timestampadd(
                SQL_TSI_HOUR,
                cast(generated_number - 1 as int),
                cast({{ start_date }} as timestamp)
            ) as date_hour
        from filtered_numbers

    )

    select *
    from all_periods
    where date_hour < cast({{ end_date }} as timestamp)

{% endmacro %}