{% macro calculate_media_giornaliera(source_cte, condition_sql, column_suffix) %}

    select
        cod_ubic,
        cod_conf,
        sigla_param,
        round(avg(val_param), 2) as media_giornaliera_{{ column_suffix }},
        count(val_param) as valori_validi_gg_{{ column_suffix }},
        cast(data_da as date) as data
    from {{ source_cte }}
    where
        {{ condition_sql }}
    group by
        cod_ubic,
        cod_conf,
        sigla_param,
        cast(data_da as date)
    having count(val_param) >= 18

    union all

    select
        cod_ubic,
        cod_conf,
        sigla_param,
        round(avg(val_param), 2) as media_giornaliera_{{ column_suffix }},
        count(val_param) as valori_validi_gg_{{ column_suffix }},
        cast(data_da as date) as data
    from {{ source_cte }}
    where
        cod_freq = 'CG'
        and {{ condition_sql }}
    group by
        cod_ubic,
        cod_conf,
        sigla_param,
        cast(data_da as date)

{% endmacro %}