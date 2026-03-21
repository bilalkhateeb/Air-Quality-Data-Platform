{{ config(materialized='view') }}

{% set validation_levels = [
    {'name': 'Dati validi', 'code': 'VS', 'hourly_cond': "cod_valid = '0'", 'mv_col': 'mv_avg_val'},
    {'name': 'Dati validi e validati COR', 'code': 'VX', 'hourly_cond': "cod_valid = '0' and cod_validaz = 'V'", 'mv_col': 'mv_avg_val_cor'},
    {'name': 'Dati certificati', 'code': 'VR', 'hourly_cond': "cod_validaz_reg = 'V'", 'mv_col': 'mv_avg_cert'}
] %}

-- Final O3 indicator definitions used in the unpivot step
{% set indicator_configs = [
    {"nome":"Ozono (O3): valore massima media oraria","rif":"","cond":"","note":"","valore":"max_val_param","teorici":"24","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): numero di medie orarie superiori al livello di informazione","rif":"soglia informazione: 180 µg/m3","cond":"","note":"","valore":"count_over_180","teorici":"24","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): numero di medie orarie superiori al livello di informazione da inizio anno","rif":"soglia informazione: 180 µg/m3","cond":"","note":"","valore":"ytd_over_180","teorici":"available_days_ytd * 24","rilevati":"ytd_count_val"},
    {"nome":"Ozono (O3): numero di medie orarie superiori al livello di allarme","rif":"soglia di allarme: 240 µg/m3","cond":"per 3 ore consecutive","note":"4 ore consecutive = 2 superi","valore":"count_superi_3h","teorici":"24","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): numero di medie orarie superiori al livello di allarme da inizio anno","rif":"soglia di allarme: 240 µg/m3","cond":"per 3 ore consecutive","note":"4 ore consecutive = 2 superi","valore":"ytd_superi_3h","teorici":"available_days_ytd * 24","rilevati":"ytd_count_val"},
    {"nome":"Ozono (O3): valore massima media 8 oraria","rif":"","cond":"","note":"","valore":"max_mv_avg","teorici":"24","rilevati":"count_mv_avg"},
    {"nome":"Ozono (O3): numero di giorni con medie su 8 ore massime giornaliere superiori al valore obiettivo a lungo termine da inizio anno","rif":"obiettivo: 120 µg/m3","cond":"","note":"","valore":"ytd_8h_over_120","teorici":"available_days_ytd","rilevati":"ytd_valid_days"}
] %}

with hourly_base as (

    select
        cast(data_da as date) as data_day,
        data_da,
        cod_ubic,
        cod_conf,
        val_param,
        cod_valid,
        cod_validaz,
        cod_validaz_reg
    from {{ ref('int_aria__data_prep') }}
    where sigla_param = 'O3'

),

mv8h_base as (

    select
        cast(data_da as date) as data_day,
        cod_ubic,
        cod_conf,
        mv_avg_val,
        mv_avg_val_cor,
        mv_avg_cert
    from {{ ref('int_aria__media_mobile_8h_cor') }}
    where sigla_param = 'O3'

),

{% for lvl in validation_levels %}

lvl_{{ lvl.code }}_hourly as (

    select
        data_day,
        data_da,
        cod_ubic,
        cod_conf,
        val_param,
        case when round(val_param) > 180 then 1 else 0 end as is_over_180,
        case when round(val_param) > 240 then 1 else 0 end as is_over_240
    from hourly_base
    where {{ lvl.hourly_cond }}

),

lvl_{{ lvl.code }}_rolling as (

    select
        data_day,
        cod_ubic,
        cod_conf,
        is_over_180,
        {{ rolling_3h_threshold_sum('is_over_240') }} as rolling_3h_over_240,
        val_param
    from lvl_{{ lvl.code }}_hourly

),

lvl_{{ lvl.code }}_daily_hourly as (

    select
        data_day,
        cod_ubic,
        cod_conf,
        count(val_param) as count_val_param,
        max(round(val_param)) as max_val_param,
        sum(is_over_180) as count_over_180,
        sum(case when rolling_3h_over_240 = 3 then 1 else 0 end) as count_superi_3h
    from lvl_{{ lvl.code }}_rolling
    group by data_day, cod_ubic, cod_conf

),

lvl_{{ lvl.code }}_daily_8h as (

    select
        data_day,
        cod_ubic,
        cod_conf,
        count({{ lvl.mv_col }}) as count_mv_avg,
        max(round({{ lvl.mv_col }})) as max_mv_avg
    from mv8h_base
    group by data_day, cod_ubic, cod_conf

),

lvl_{{ lvl.code }}_daily as (

    select
        h.data_day,
        h.cod_ubic,
        h.cod_conf,
        h.count_val_param,
        h.max_val_param,
        h.count_over_180,
        h.count_superi_3h,
        min(h.data_day) over (
            partition by extract(year from h.data_day), h.cod_ubic, h.cod_conf
        ) as first_available_day_in_year,
        coalesce(m.count_mv_avg, 0) as count_mv_avg,
        m.max_mv_avg
    from lvl_{{ lvl.code }}_daily_hourly h
    left join lvl_{{ lvl.code }}_daily_8h m
      on h.data_day = m.data_day
     and h.cod_ubic = m.cod_ubic
     and h.cod_conf = m.cod_conf

),

lvl_{{ lvl.code }}_metrics as (

    select
        *,
        {{ available_year_elapsed_days('data_day', 'first_available_day_in_year') }} as available_days_ytd,
        {{ calc_validita_indicatore('count_mv_avg', '24') }} as validita_8h,

        {{ ytd_sum('count_over_180') }} as ytd_over_180,
        {{ ytd_sum('count_superi_3h') }} as ytd_superi_3h,
        {{ ytd_sum('count_val_param') }} as ytd_count_val,

        {{ ytd_sum("case when max_mv_avg > 120 and validita_8h = 'Si' then 1 else 0 end") }} as ytd_8h_over_120,
        {{ ytd_sum("case when validita_8h = 'Si' then 1 else 0 end") }} as ytd_valid_days
    from lvl_{{ lvl.code }}_daily

),

lvl_{{ lvl.code }}_unpivoted as (

    {% for ind in indicator_configs %}
    select
        data_day as data,
        cod_ubic,
        cod_conf,
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
    from lvl_{{ lvl.code }}_metrics
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

){% if not loop.last %},{% endif %}

{% endfor %}

select * from lvl_VS_unpivoted
union all
select * from lvl_VX_unpivoted
union all
select * from lvl_VR_unpivoted