with source as (

    select *
    from {{ source('aria', 'rqa_parametri') }}

),

renamed as (

    select
        cast(COD_PARAM as varchar)      as cod_param,
        cast(DESCR_PARAM as varchar)    as descr_param,
        cast(SIGLA_PARAM as varchar)    as sigla_param,
        cast(COD_METODO as varchar)     as cod_metodo,
        cast(FLAG_PUBBLICO as varchar)  as flag_pubblico,
        cast(COD_ELEMENTO as varchar)   as cod_elemento
    from source

)

select *
from renamed