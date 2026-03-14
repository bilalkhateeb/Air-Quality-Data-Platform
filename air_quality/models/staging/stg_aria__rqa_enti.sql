with source as (

    select *
    from {{ source('aria', 'rqa_enti') }}

),

renamed as (

    select
        cast(COD_ENTE as varchar)      as cod_ente,
        cast(DESCR_ENTE as varchar)    as descr_ente,
        cast(T_RETE as varchar)        as t_rete,
        cast(FLAG_PRIVATA as varchar)  as flag_privata
    from source

)

select *
from renamed