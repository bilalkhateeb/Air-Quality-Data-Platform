with source as (

    select *
    from {{ source('aria', 'rqa_configurazioni') }}

),

renamed as (

    select
        cast(COD_CONF as integer)       as cod_conf,
        cast(COD_UBIC as varchar)       as cod_ubic,
        cast(COD_PARAM as varchar)      as cod_param,
        cast(COD_ENTE as varchar)       as cod_ente,
        cast(COD_MIS as varchar)        as cod_mis,
        cast(COD_FREQ as varchar)       as cod_freq,
        cast(STATION_CL_EU as varchar)  as station_cl_eu,
        cast(FLG_PUB as varchar)        as flg_pub
    from source

)

select *
from renamed