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

)
select
    data,
    cod_ubic,
    cod_conf,
    sigla_param,
    val.media_giornaliera_val,
    val.valori_validi_gg_val,
    cor.media_giornaliera_val_cor,
    cor.valori_validi_gg_val_cor,
    cert.media_giornaliera_cert,
    cert.valori_validi_gg_cert
from media_gg_val val
full outer join media_gg_val_cor cor
    using (data, cod_ubic, cod_conf, sigla_param)
full outer join media_gg_cert cert
    using (data, cod_ubic, cod_conf, sigla_param)