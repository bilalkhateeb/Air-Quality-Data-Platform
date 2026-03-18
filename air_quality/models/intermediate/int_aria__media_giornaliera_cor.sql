with data_prep as (

    select
        cod_ubic,
        cod_conf,
        sigla_param,
        cod_freq,
        data_da,
        val_param,
        cod_valid,
        cod_validaz,
        cod_validaz_reg
    from {{ ref('int_aria__data_prep') }}

),

media_gg_val as (

    {{ calculate_media_giornaliera('data_prep', "cod_valid = '0'", "val") }}

),

media_gg_val_cor as (

    {{ calculate_media_giornaliera('data_prep', "cod_valid = '0' and cod_validaz = 'V'", "val_cor") }}

),

media_gg_cert as (

    {{ calculate_media_giornaliera('data_prep', "cod_validaz_reg = 'V'", "cert") }}

),

val_cor_joined as (

    select
        coalesce(val.data, cor.data) as data,
        coalesce(val.cod_ubic, cor.cod_ubic) as cod_ubic,
        coalesce(val.cod_conf, cor.cod_conf) as cod_conf,
        coalesce(val.sigla_param, cor.sigla_param) as sigla_param,
        val.media_giornaliera_val,
        val.valori_validi_gg_val,
        cor.media_giornaliera_val_cor,
        cor.valori_validi_gg_val_cor
    from media_gg_val val
    full outer join media_gg_val_cor cor
        on val.data = cor.data
       and val.cod_ubic = cor.cod_ubic
       and val.cod_conf = cor.cod_conf
       and val.sigla_param = cor.sigla_param

)

select
    coalesce(vc.data, cert.data) as data,
    coalesce(vc.cod_ubic, cert.cod_ubic) as cod_ubic,
    coalesce(vc.cod_conf, cert.cod_conf) as cod_conf,
    coalesce(vc.sigla_param, cert.sigla_param) as sigla_param,
    vc.media_giornaliera_val,
    vc.valori_validi_gg_val,
    vc.media_giornaliera_val_cor,
    vc.valori_validi_gg_val_cor,
    cert.media_giornaliera_cert,
    cert.valori_validi_gg_cert
from val_cor_joined vc
full outer join media_gg_cert cert
    on vc.data = cert.data
   and vc.cod_ubic = cert.cod_ubic
   and vc.cod_conf = cert.cod_conf
   and vc.sigla_param = cert.sigla_param