{{ config(materialized='table') }}

with dati_aria as (

    select
        cod_ubic,
        cod_conf,
        data_da,
        data_a,
        val_param,
        cod_valid,
        anno
    from {{ ref('stg_aria__rqa_dati_aria_cor') }}

),

configurazioni as (

    select
        cod_conf,
        cod_ubic,
        cod_param,
        cod_freq
    from {{ ref('stg_aria__rqa_configurazioni') }}

),

parametri as (

    select
        cod_param,
        sigla_param
    from {{ ref('stg_aria__rqa_parametri') }}

),

joined as (

    select
        d.cod_ubic,
        d.cod_conf,
        c.cod_param,
        d.data_da,
        d.data_a,
        d.val_param,
        d.cod_valid,
        d.anno,
        c.cod_freq,
        p.sigla_param
    from dati_aria d
    inner join configurazioni c
        on d.cod_ubic = c.cod_ubic
       and d.cod_conf = c.cod_conf
    inner join parametri p
        on c.cod_param = p.cod_param

)

select
    cod_ubic,
    cod_conf,
    cod_param,
    data_da,
    data_a,
    val_param,
    cod_valid,
    anno,
    cod_freq,
    sigla_param
from joined
