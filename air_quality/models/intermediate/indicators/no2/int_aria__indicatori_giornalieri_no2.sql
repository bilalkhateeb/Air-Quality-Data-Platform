{{ config(materialized='view') }}

{% set validation_levels = [
    {
        'name': 'Dati validi',
        'code': 'VS',
        'hourly_cond': "cod_valid = '0'",
        'mean_col': 'media_giornaliera_val',
        'mean_count_col': 'valori_validi_gg_val'
    },
    {
        'name': 'Dati validi e validati COR',
        'code': 'VX',
        'hourly_cond': "cod_valid = '0' and cod_validaz = 'V'",
        'mean_col': 'media_giornaliera_val_cor',
        'mean_count_col': 'valori_validi_gg_val_cor'
    },
    {
        'name': 'Dati certificati',
        'code': 'VR',
        'hourly_cond': "cod_validaz_reg = 'V'",
        'mean_col': 'media_giornaliera_cert',
        'mean_count_col': 'valori_validi_gg_cert'
    }
] %}

{% set hourly_indicator_configs = [
    {"nome":"Biossido di azoto (NO2): numero di medie orarie superiori al valore limite orario","rif":"200 µg/m3","cond":"","note":"","valore":"count_over_200","teorici":"24","rilevati":"count_val_param"},
    {"nome":"Biossido di azoto (NO2): numero di medie orarie superiori al valore limite orario da inizio anno","rif":"200 µg/m3","cond":"","note":"","valore":"ytd_over_200","teorici":"available_days_ytd * 24","rilevati":"ytd_count_val"},
    {"nome":"Biossido di azoto (NO2): valore massimo orario","rif":"","cond":"","note":"","valore":"max_val_param","teorici":"24","rilevati":"count_val_param"},
    {"nome":"Biossido di azoto (NO2): numero di superamenti della soglia di allarme","rif":"soglia di allarme: 400 µg/m3","cond":"per 3 ore consecutive","note":"4 ore consecutive = 2 superi","valore":"count_superi_3h","teorici":"24","rilevati":"count_val_param"}
] %}

with hourly_base as (

    select
        cast(data_da as date) as data_day,
        data_da,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        cod_valid,
        cod_validaz,
        cod_validaz_reg
    from {{ ref('int_aria__data_prep') }}
    where sigla_param = 'NO2'

),

daily_mean_no2 as (

    select
        data as data_day,
        cod_ubic,
        cod_conf,
        sigla_param,
        media_giornaliera_val,
        valori_validi_gg_val,
        media_giornaliera_val_cor,
        valori_validi_gg_val_cor,
        media_giornaliera_cert,
        valori_validi_gg_cert
    from {{ ref('int_aria__media_giornaliera_cor') }}
    where sigla_param = 'NO2'

),

{% for lvl in validation_levels %}

lvl_{{ lvl.code }}_hourly as (

    select
        data_day,
        data_da,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        case when round(val_param) > 200 then 1 else 0 end as is_over_200,
        case when round(val_param) > 400 then 1 else 0 end as is_over_400
    from hourly_base
    where {{ lvl.hourly_cond }}

),

lvl_{{ lvl.code }}_rolling as (

    select
        data_day,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        is_over_200,
        {{ rolling_3h_threshold_sum('is_over_400') }} as rolling_3h_over_400
    from lvl_{{ lvl.code }}_hourly

),

lvl_{{ lvl.code }}_daily_hourly as (

    select
        data_day,
        cod_ubic,
        cod_conf,
        sigla_param,
        count(val_param) as count_val_param,
        max(round(val_param)) as max_val_param,
        sum(is_over_200) as count_over_200,
        sum(case when rolling_3h_over_400 = 3 then 1 else 0 end) as count_superi_3h,
        min(data_day) over (
            partition by extract(year from data_day), cod_ubic, cod_conf, sigla_param
        ) as first_available_day_in_year
    from lvl_{{ lvl.code }}_rolling
    group by data_day, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_hourly_metrics as (

    select
        *,
        {{ available_year_elapsed_days('data_day', 'first_available_day_in_year') }} as available_days_ytd,
        {{ ytd_sum('count_over_200') }} as ytd_over_200,
        {{ ytd_sum('count_val_param') }} as ytd_count_val
    from lvl_{{ lvl.code }}_daily_hourly

),

lvl_{{ lvl.code }}_hourly_unpivoted as (

    {% for ind in hourly_indicator_configs %}
    select
        data_day as data,
        cod_ubic,
        cod_conf,
        sigla_param,
        '{{ lvl.name }}' as livello_validazione,
        '{{ lvl.code }}' as cod_liv_validazione,
        {{ generate_indicator_row(
            ind.nome,
            ind.rif,
            ind.cond,
            ind.note,
            ind.valore,
            ind.teorici,
            ind.rilevati
        ) }}
    from lvl_{{ lvl.code }}_hourly_metrics
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

),

lvl_{{ lvl.code }}_daily_mean_unpivoted as (

    select
        h.data_day as data,
        h.cod_ubic,
        h.cod_conf,
        h.sigla_param,
        '{{ lvl.name }}' as livello_validazione,
        '{{ lvl.code }}' as cod_liv_validazione,
        {{ generate_indicator_row(
            'Biossido di azoto (NO2): valore media giornaliera',
            '',
            '',
            '',
            'round(m.' ~ lvl.mean_col ~ ')',
            '24',
            'm.' ~ lvl.mean_count_col
        ) }}
    from (
        select data_day, cod_ubic, cod_conf, sigla_param
        from lvl_{{ lvl.code }}_daily_hourly
    ) h
    left join daily_mean_no2 m
        on h.data_day = m.data_day
       and h.cod_ubic = m.cod_ubic
       and h.cod_conf = m.cod_conf
       and h.sigla_param = m.sigla_param

),

lvl_{{ lvl.code }}_unpivoted as (

    select * from lvl_{{ lvl.code }}_hourly_unpivoted
    union all
    select * from lvl_{{ lvl.code }}_daily_mean_unpivoted

)

{% if not loop.last %},{% endif %}
{% endfor %}

select * from lvl_VS_unpivoted
union all
select * from lvl_VX_unpivoted
union all
select * from lvl_VR_unpivoted