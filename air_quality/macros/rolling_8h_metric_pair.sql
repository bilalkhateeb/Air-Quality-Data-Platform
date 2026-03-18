{% macro rolling_8h_metric_pair(condition_sql, avg_alias, count_alias) %}
    round(
        avg(
            case
                when {{ condition_sql }} then val_param
            end
        ) over (
            partition by cod_ubic, cod_conf, sigla_param
            order by data_da
            rows between 7 preceding and current row
        ),
        2
    ) as {{ avg_alias }},

    count(
        case
            when {{ condition_sql }} then val_param
        end
    ) over (
        partition by cod_ubic, cod_conf, sigla_param
        order by data_da
        rows between 7 preceding and current row
    ) as {{ count_alias }}
{% endmacro %}