{{ config(materialized='table') }}

with reti as (

    select
        t_rete,
        descr_t_rete
    from {{ ref('stg_aria__rqa_t_reti') }}

),

enti as (

    select
        cod_ente,
        descr_ente,
        t_rete,
        flag_privata
    from {{ ref('stg_aria__rqa_enti') }}

),

configurazioni as (

    select
        cod_conf,
        cod_ubic,
        cod_param,
        cod_ente,
        cod_mis,
        cod_freq,
        station_cl_eu,
        flg_pub
    from {{ ref('stg_aria__rqa_configurazioni') }}

),

parametri as (

    select
        cod_param,
        descr_param,
        sigla_param,
        cod_metodo,
        flag_pubblico,
        cod_elemento
    from {{ ref('stg_aria__rqa_parametri') }}

),

ubicazioni as (

    select
        cod_ubic,
        descr_ubic,
        via_ubic,
        cod_prov,
        cod_com,
        cmirl_lat,
        cmirl_lon,
        data_disattiv,
        cod_eu,
        cod_tzona
    from {{ ref('stg_aria__rqa_ubicazioni') }}

),

unita_misura as (

    select
        cod_mis,
        sigla_mis
    from {{ ref('stg_aria__t_unita_misura') }}

),

frequenze as (

    select
        cod_freq,
        descr_freq
    from {{ ref('stg_aria__rqa_t_frequenze') }}

),

province as (

    select
        cod_prov,
        nom_prov
    from {{ ref('stg_aria__province') }}

),

comuni as (

    select
        cod_com,
        cod_prov,
        nom_com
    from {{ ref('stg_aria__comuni') }}

),

classi_eu as (

    select
        station_cl_eu,
        desc_eu
    from {{ ref('stg_aria__rqa_t_stclass_eu') }}

),

elementi as (

    select
        cod_elemento,
        inquinante
    from {{ ref('stg_aria__rqa_elementi') }}

),

metodi as (

    select
        cod_metodo,
        metodo
    from {{ ref('stg_aria__rqa_metodi') }}

),

zone as (

    select
        cod_tzona,
        tipo_zona
    from {{ ref('stg_aria__rqa_t_zona') }}

),

joined as (

    select
        u.cod_ubic,
        u.descr_ubic,
        u.via_ubic,
        u.cod_prov,
        pr.nom_prov,
        u.cod_com,
        co.nom_com,
        u.cmirl_lat,
        u.cmirl_lon,
        u.data_disattiv,
        u.cod_eu,
        u.cod_tzona,
        z.tipo_zona,

        c.cod_conf,
        c.cod_param,
        c.cod_ente,
        e.descr_ente,
        e.t_rete,
        r.descr_t_rete,
        e.flag_privata,
        c.cod_mis,
        um.sigla_mis,
        c.cod_freq,
        f.descr_freq,
        c.station_cl_eu,
        ce.desc_eu,
        c.flg_pub,

        p.descr_param,
        p.sigla_param,
        p.cod_metodo,
        m.metodo,
        p.flag_pubblico,
        p.cod_elemento,
        el.inquinante

    from configurazioni c
    inner join enti e
        on c.cod_ente = e.cod_ente
    inner join reti r
        on e.t_rete = r.t_rete
    inner join parametri p
        on c.cod_param = p.cod_param
    inner join ubicazioni u
        on c.cod_ubic = u.cod_ubic
    inner join unita_misura um
        on c.cod_mis = um.cod_mis
    inner join frequenze f
        on c.cod_freq = f.cod_freq
    inner join province pr
        on u.cod_prov = pr.cod_prov
    left join comuni co
        on u.cod_com = co.cod_com
       and u.cod_prov = co.cod_prov
    left join classi_eu ce
        on c.station_cl_eu = ce.station_cl_eu
    inner join elementi el
        on p.cod_elemento = el.cod_elemento
    inner join metodi m
        on p.cod_metodo = m.cod_metodo
    left join zone z
        on u.cod_tzona = z.cod_tzona

)

select
    cod_ubic,
    descr_ubic,
    via_ubic,
    cod_prov,
    nom_prov,
    cod_com,
    nom_com,
    cmirl_lat,
    cmirl_lon,
    data_disattiv,
    cod_eu,
    cod_tzona,
    tipo_zona,
    cod_conf,
    cod_param,
    cod_ente,
    descr_ente,
    t_rete,
    descr_t_rete,
    flag_privata,
    cod_mis,
    sigla_mis,
    cod_freq,
    descr_freq,
    station_cl_eu,
    desc_eu,
    flg_pub,
    descr_param,
    sigla_param,
    cod_metodo,
    metodo,
    flag_pubblico,
    cod_elemento,
    inquinante
from joined
