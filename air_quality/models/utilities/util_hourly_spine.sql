with hourly_spine as (

    {{
        dbt.date_spine(
            datepart="hour",
            start_date="cast('2020-10-16 00:00:00' as timestamp)",
            end_date="cast('2021-04-04 00:00:00' as timestamp)"
        )
    }}

),

final as (

    select
        cast(date_hour as timestamp) as data_da,
        cast(date_hour as timestamp) + interval '1' hour as data_a
    from hourly_spine

)

select
    data_da,
    data_a
from final