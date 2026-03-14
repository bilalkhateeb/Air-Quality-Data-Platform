with source as (

    select *
    from {{ source('aria', 'province') }}

),

renamed as (

    select
        cast(COD_PROV as varchar)  as cod_prov,
        cast(NOM_PROV as varchar)  as nom_prov
    from source

)

select *
from renamed