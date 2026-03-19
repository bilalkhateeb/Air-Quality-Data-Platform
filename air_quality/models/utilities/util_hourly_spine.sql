with hourly_spine as (

    {{
        dbt_utils.date_spine(
            datepart="hour",
            start_date="cast('2020-11-16 00:00:00' as timestamp)",
            end_date="cast('2021-03-04 00:00:00' as timestamp)"
        )
    }}

),

final as (

    select
        cast(date_hour as timestamp) as data_da,
        timestampadd(SQL_TSI_HOUR, 1, cast(date_hour as timestamp)) as data_a
    from hourly_spine

)

select
    data_da,
    data_a
from final