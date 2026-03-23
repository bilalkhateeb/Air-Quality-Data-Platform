{{ config(materialized='view') }}

{% set validation_levels = [
    {'name': 'Dati validi', 'code': 'VS', 'hourly_cond': "cod_valid = '0'", 'mv_col': 'mv_avg_val'},
    {'name': 'Dati validi e validati COR', 'code': 'VX', 'hourly_cond': "cod_valid = '0' and cod_validaz = 'V'", 'mv_col': 'mv_avg_val_cor'},
    {'name': 'Dati certificati', 'code': 'VR', 'hourly_cond': "cod_validaz_reg = 'V'", 'mv_col': 'mv_avg_cert'}
] %}

{% set indicator_configs = [
    {"nome":"Ozono (O3): numero di giorni con medie su 8 ore massime giornaliere superiori al valore obiettivo a lungo termine","limite":"120 µg/m3","cond":"","critico":"","valore":"days_over_120","teorici":"available_days_in_year","rilevati":"valid_days_8h"},
    {"nome":"Ozono (O3): numero di medie orarie superiori alla soglia di informazione","limite":"180 µg/m3","cond":"","critico":"","valore":"count_over_180","teorici":"available_hours_in_year","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): numero di medie orarie superiori alla soglia di allarme","limite":"240 µg/m3","cond":"x 3 ore consecutive","critico":"","valore":"count_superi_3h","teorici":"available_hours_in_year","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): valore media annuale","limite":"","cond":"","critico":"","valore":"mean_val_param","teorici":"available_hours_in_year","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): 98° percentile","limite":"","cond":"","critico":"","valore":"p98_val_param","teorici":"available_hours_in_year","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): deviazione standard","limite":"","cond":"","critico":"","valore":"stddev_val_param","teorici":"available_hours_in_year","rilevati":"count_val_param"},
    {"nome":"Ozono (O3): mediana","limite":"","cond":"","critico":"","valore":"median_val_param","teorici":"available_hours_in_year","rilevati":"count_val_param"}
] %}

with hourly_base as (

    select
        cast(data_da as date) as data_day,
        extract(year from data_da) as anno,
        data_da,
        cod_ubic,
        cod_conf,
        sigla_param,
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
        extract(year from data_da) as anno,
        cod_ubic,
        cod_conf,
        sigla_param,
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
        anno,
        data_da,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        case when round(val_param) > 180 then 1 else 0 end as is_over_180,
        case when round(val_param) > 240 then 1 else 0 end as is_over_240
    from hourly_base
    where {{ lvl.hourly_cond }}

),

lvl_{{ lvl.code }}_rolling as (

    select
        data_day,
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        is_over_180,
        {{ rolling_3h_threshold_sum('is_over_240') }} as rolling_3h_over_240,
        val_param
    from lvl_{{ lvl.code }}_hourly

),

lvl_{{ lvl.code }}_year_bounds as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        min(data_day) as first_available_day_in_year,
        max(data_day) as last_available_day_in_year
    from lvl_{{ lvl.code }}_hourly
    group by anno, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_annual_hourly as (

    select
        r.anno,
        r.cod_ubic,
        r.cod_conf,
        r.sigla_param,
        count(r.val_param) as count_val_param,
        sum(r.is_over_180) as count_over_180,
        sum(case when r.rolling_3h_over_240 = 3 then 1 else 0 end) as count_superi_3h,
        round(avg(r.val_param)) as mean_val_param,
        round(percentile_cont(0.98) within group (order by r.val_param)) as p98_val_param,
        round(stddev_samp(r.val_param)) as stddev_val_param,
        round(percentile_cont(0.5) within group (order by r.val_param)) as median_val_param
    from lvl_{{ lvl.code }}_rolling r
    group by r.anno, r.cod_ubic, r.cod_conf, r.sigla_param

),

lvl_{{ lvl.code }}_daily_8h as (

    select
        anno,
        data_day,
        cod_ubic,
        cod_conf,
        sigla_param,
        count({{ lvl.mv_col }}) as count_mv_avg,
        max(round({{ lvl.mv_col }})) as max_mv_avg
    from mv8h_base
    group by anno, data_day, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_annual_8h as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        sum(case when max_mv_avg > 120 and count_mv_avg >= 18 then 1 else 0 end) as days_over_120,
        sum(case when count_mv_avg >= 18 then 1 else 0 end) as valid_days_8h
    from lvl_{{ lvl.code }}_daily_8h
    group by anno, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_annual as (

    select
        h.anno,
        h.cod_ubic,
        h.cod_conf,
        h.sigla_param,
        h.count_val_param,
        h.count_over_180,
        h.count_superi_3h,
        h.mean_val_param,
        h.p98_val_param,
        h.stddev_val_param,
        h.median_val_param,
        coalesce(a8.days_over_120, 0) as days_over_120,
        coalesce(a8.valid_days_8h, 0) as valid_days_8h,
        y.first_available_day_in_year,
        y.last_available_day_in_year,
        {{ available_year_hourly_theoretical('y.first_available_day_in_year', 'y.last_available_day_in_year') }} as available_hours_in_year,
        datediff(cast(y.last_available_day_in_year as date), cast(y.first_available_day_in_year as date)) + 1 as available_days_in_year
    from lvl_{{ lvl.code }}_annual_hourly h
    left join lvl_{{ lvl.code }}_annual_8h a8
        on h.anno = a8.anno
       and h.cod_ubic = a8.cod_ubic
       and h.cod_conf = a8.cod_conf
       and h.sigla_param = a8.sigla_param
    left join lvl_{{ lvl.code }}_year_bounds y
        on h.anno = y.anno
       and h.cod_ubic = y.cod_ubic
       and h.cod_conf = y.cod_conf
       and h.sigla_param = y.sigla_param

),

lvl_{{ lvl.code }}_unpivoted as (

    {% for ind in indicator_configs %}
    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        '{{ lvl.name }}' as livello_validazione,
        '{{ lvl.code }}' as cod_liv_validazione,
        {{ generate_annual_indicator_row(
            ind.nome,
            ind.limite,
            ind.cond,
            ind.critico,
            ind.valore,
            ind.teorici,
            ind.rilevati
        ) }}
    from lvl_{{ lvl.code }}_annual
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

)

{% if not loop.last %},{% endif %}
{% endfor %}

select * from lvl_VS_unpivoted
union all
select * from lvl_VX_unpivoted
union all
select * from lvl_VR_unpivoted