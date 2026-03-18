with data_prep as (

    select
        cod_ubic,
        cod_conf,
        sigla_param,
        data_da,
        data_a,
        val_param,
        cod_valid,
        cod_validaz,
        cod_validaz_reg
    from {{ ref('int_aria__data_prep') }}
    where sigla_param in ('CO', 'O3')

),

pairs as (

    select distinct
        cod_ubic,
        cod_conf,
        sigla_param
    from data_prep

),

hourly_joined as (

    select
        s.data_da,
        s.data_a,
        p.cod_ubic,
        p.cod_conf,
        p.sigla_param,
        d.val_param,
        d.cod_valid,
        d.cod_validaz,
        d.cod_validaz_reg
    from {{ ref('util_hourly_spine') }} s
    cross join pairs p
    left join data_prep d
        on s.data_da = d.data_da
       and s.data_a = d.data_a
       and p.cod_ubic = d.cod_ubic
       and p.cod_conf = d.cod_conf
       and p.sigla_param = d.sigla_param

),

windowed as (

    select
        data_da,
        data_a,
        cod_ubic,
        cod_conf,
        sigla_param,

        {{ rolling_8h_metric_pair(
            "cod_valid = '0'",
            "mv_avg_val",
            "valori_validi_mv_avg_val"
        ) }},

        {{ rolling_8h_metric_pair(
            "cod_valid = '0' and cod_validaz = 'V'",
            "mv_avg_val_cor",
            "valori_validi_mv_avg_val_cor"
        ) }},

        {{ rolling_8h_metric_pair(
            "cod_validaz_reg = 'V'",
            "mv_avg_cert",
            "valori_validi_mv_avg_cert"
        ) }}

    from hourly_joined

),

final as (

    select
        data_da,
        data_a,
        cod_ubic,
        cod_conf,
        sigla_param,

        {{ apply_8h_threshold_pair(
            "mv_avg_val",
            "valori_validi_mv_avg_val",
            "mv_avg_val",
            "valori_validi_mv_avg_val"
        ) }},

        {{ apply_8h_threshold_pair(
            "mv_avg_val_cor",
            "valori_validi_mv_avg_val_cor",
            "mv_avg_val_cor",
            "valori_validi_mv_avg_val_cor"
        ) }},

        {{ apply_8h_threshold_pair(
            "mv_avg_cert",
            "valori_validi_mv_avg_cert",
            "mv_avg_cert",
            "valori_validi_mv_avg_cert"
        ) }}

    from windowed

)

select
    data_da,
    data_a,
    cod_ubic,
    cod_conf,
    sigla_param,
    mv_avg_val,
    valori_validi_mv_avg_val,
    mv_avg_val_cor,
    valori_validi_mv_avg_val_cor,
    mv_avg_cert,
    valori_validi_mv_avg_cert
from final
where
    mv_avg_val is not null
    or mv_avg_val_cor is not null
    or mv_avg_cert is not null