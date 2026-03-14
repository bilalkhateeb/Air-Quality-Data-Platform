with source as (

    select *
    from {{ source('aria', 'rqa_ubicazioni') }}

),

renamed as (

    select
        cast(COD_UBIC as varchar)   as cod_ubic,
        cast(DESCR_UBIC as varchar) as descr_ubic,
        cast(VIA_UBIC as varchar)   as via_ubic,
        cast(COD_PROV as varchar)   as cod_prov,
        cast(COD_COM as varchar)    as cod_com,

        cast(nullif(CMIRL_LAT, '') as double) as cmirl_lat,
        cast(nullif(CMIRL_LON, '') as double) as cmirl_lon,

        cast(nullif(DATA_DISATTIV, '') as timestamp) as data_disattiv,

        cast(COD_EU as varchar)     as cod_eu,
        cast(COD_TZONA as varchar)  as cod_tzona
    from source

)

select *
from renamed