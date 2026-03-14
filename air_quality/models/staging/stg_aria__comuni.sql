with source as (

    select *
    from {{ source('aria', 'comuni') }}

),

renamed as (

    select
        cast(COD_COM as varchar)   as cod_com,
        cast(NOM_COM as varchar)   as nom_com,
        cast(COD_PROV as varchar)  as cod_prov
    from source

)

select *
from renamed