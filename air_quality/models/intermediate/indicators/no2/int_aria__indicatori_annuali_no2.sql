{{ config(materialized='view') }}

{% set validation_levels = [
    {'name': 'Dati validi', 'code': 'VS', 'hourly_cond': "cod_valid = '0'"},
    {'name': 'Dati validi e validati COR', 'code': 'VX', 'hourly_cond': "cod_valid = '0' and cod_validaz = 'V'"},
    {'name': 'Dati certificati', 'code': 'VR', 'hourly_cond': "cod_validaz_reg = 'V'"}
] %}

{% set no2_indicator_configs = [
    {"nome":"Biossido di azoto (NO2): numero di medie orarie superiori al limite","limite":"200 µg/m3","cond":"max 18/anno","critico":"","valore":"count_over_200","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): numero di medie orarie superiori alla soglia UAT","limite":"140 µg/m3","cond":"max 18/anno","critico":"","valore":"count_over_140","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): numero di medie orarie superiori alla soglia LAT","limite":"100 µg/m3","cond":"max 18/anno","critico":"","valore":"count_over_100","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): valore massima media oraria","limite":"","cond":"","critico":"","valore":"max_no2","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): numero di superamenti della soglia di allarme","limite":"400 µg/m3","cond":"x3 ore consecutive","critico":"","valore":"count_alarm_3h","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): valore media annuale","limite":"","cond":"","critico":"livello critico: 40 µg/m3, UAT: 32 µg/m3, LAT: 26 µg/m3","valore":"avg_no2","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): 98° percentile","limite":"","cond":"","critico":"","valore":"p98_no2","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): deviazione standard","limite":"","cond":"","critico":"","valore":"stddev_no2","teorici":"available_hours_in_year","rilevati":"count_no2"},
    {"nome":"Biossido di azoto (NO2): mediana","limite":"","cond":"","critico":"","valore":"median_no2","teorici":"available_hours_in_year","rilevati":"count_no2"}
] %}

{% set nox_indicator_config = {
    "nome":"Ossidi di azoto (NOx): valore media annuale",
    "limite":"",
    "cond":"",
    "critico":"livello critico: 30 µg/m3 NO2, UAT: 24 µg/m3 NO2, LAT: 19.5 µg/m3 NO2",
    "valore":"avg_nox_as_no2",
    "teorici":"available_hours_in_year_nox",
    "rilevati":"count_nox"
} %}

with no2_hourly_base as (

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
    where sigla_param = 'NO2'

),

nox_hourly_base as (

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
    where sigla_param = 'NOX'

),

{% for lvl in validation_levels %}

lvl_{{ lvl.code }}_no2_hourly as (

    select
        data_day,
        anno,
        data_da,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        case when round(val_param) > 100 then 1 else 0 end as is_over_100,
        case when round(val_param) > 140 then 1 else 0 end as is_over_140,
        case when round(val_param) > 200 then 1 else 0 end as is_over_200,
        case when round(val_param) > 400 then 1 else 0 end as is_over_400
    from no2_hourly_base
    where {{ lvl.hourly_cond }}

),

lvl_{{ lvl.code }}_no2_rolling as (

    select
        data_day,
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param,
        is_over_100,
        is_over_140,
        is_over_200,
        {{ rolling_3h_threshold_sum('is_over_400') }} as rolling_3h_over_400
    from lvl_{{ lvl.code }}_no2_hourly

),

lvl_{{ lvl.code }}_no2_year_bounds as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        min(data_day) as first_available_day_in_year,
        max(data_day) as last_available_day_in_year
    from lvl_{{ lvl.code }}_no2_hourly
    group by anno, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_no2_annual as (

    select
        r.anno,
        r.cod_ubic,
        r.cod_conf,
        r.sigla_param,
        count(r.val_param) as count_no2,
        sum(r.is_over_200) as count_over_200,
        sum(r.is_over_140) as count_over_140,
        sum(r.is_over_100) as count_over_100,
        sum(case when r.rolling_3h_over_400 = 3 then 1 else 0 end) as count_alarm_3h,
        round(max(r.val_param)) as max_no2,
        round(avg(r.val_param)) as avg_no2,
        round(percentile_cont(0.98) within group (order by r.val_param)) as p98_no2,
        round(stddev_samp(r.val_param), 2) as stddev_no2,
        round(percentile_cont(0.5) within group (order by r.val_param)) as median_no2
    from lvl_{{ lvl.code }}_no2_rolling r
    group by r.anno, r.cod_ubic, r.cod_conf, r.sigla_param

),

lvl_{{ lvl.code }}_no2_final as (

    select
        a.anno,
        a.cod_ubic,
        a.cod_conf,
        a.sigla_param,
        a.count_no2,
        a.count_over_200,
        a.count_over_140,
        a.count_over_100,
        a.count_alarm_3h,
        a.max_no2,
        a.avg_no2,
        a.p98_no2,
        a.stddev_no2,
        a.median_no2,
        {{ available_year_hourly_theoretical('b.first_available_day_in_year', 'b.last_available_day_in_year') }} as available_hours_in_year
    from lvl_{{ lvl.code }}_no2_annual a
    left join lvl_{{ lvl.code }}_no2_year_bounds b
        on a.anno = b.anno
       and a.cod_ubic = b.cod_ubic
       and a.cod_conf = b.cod_conf
       and a.sigla_param = b.sigla_param

),

lvl_{{ lvl.code }}_no2_unpivoted as (

    {% for ind in no2_indicator_configs %}
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
    from lvl_{{ lvl.code }}_no2_final
    {% if not loop.last %} union all {% endif %}
    {% endfor %}

),

lvl_{{ lvl.code }}_nox_hourly as (

    select
        anno,
        data_day,
        cod_ubic,
        cod_conf,
        sigla_param,
        val_param
    from nox_hourly_base
    where {{ lvl.hourly_cond }}

),

lvl_{{ lvl.code }}_nox_year_bounds as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        min(data_day) as first_available_day_in_year_nox,
        max(data_day) as last_available_day_in_year_nox
    from lvl_{{ lvl.code }}_nox_hourly
    group by anno, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_nox_annual as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        count(val_param) as count_nox,
        round(avg(val_param) * 1.912, 1) as avg_nox_as_no2
    from lvl_{{ lvl.code }}_nox_hourly
    group by anno, cod_ubic, cod_conf, sigla_param

),

lvl_{{ lvl.code }}_nox_final as (

    select
        a.anno,
        a.cod_ubic,
        a.cod_conf,
        a.sigla_param,
        a.count_nox,
        a.avg_nox_as_no2,
        {{ available_year_hourly_theoretical('b.first_available_day_in_year_nox', 'b.last_available_day_in_year_nox') }} as available_hours_in_year_nox
    from lvl_{{ lvl.code }}_nox_annual a
    left join lvl_{{ lvl.code }}_nox_year_bounds b
        on a.anno = b.anno
       and a.cod_ubic = b.cod_ubic
       and a.cod_conf = b.cod_conf
       and a.sigla_param = b.sigla_param

),

lvl_{{ lvl.code }}_nox_unpivoted as (

    select
        anno,
        cod_ubic,
        cod_conf,
        sigla_param,
        '{{ lvl.name }}' as livello_validazione,
        '{{ lvl.code }}' as cod_liv_validazione,
        {{ generate_annual_indicator_row(
            nox_indicator_config.nome,
            nox_indicator_config.limite,
            nox_indicator_config.cond,
            nox_indicator_config.critico,
            nox_indicator_config.valore,
            nox_indicator_config.teorici,
            nox_indicator_config.rilevati
        ) }}
    from lvl_{{ lvl.code }}_nox_final

)

{% if not loop.last %},{% endif %}
{% endfor %}

select * from lvl_VS_no2_unpivoted
union all
select * from lvl_VS_nox_unpivoted
union all
select * from lvl_VX_no2_unpivoted
union all
select * from lvl_VX_nox_unpivoted
union all
select * from lvl_VR_no2_unpivoted
union all
select * from lvl_VR_nox_unpivoted