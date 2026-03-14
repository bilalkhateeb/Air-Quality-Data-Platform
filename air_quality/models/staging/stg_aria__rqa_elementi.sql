with source as (

    select *
    from {{ source('aria', 'rqa_elementi') }}

),

renamed as (

    select
        cast(COD_ELEMENTO as varchar)  as cod_elemento,
        cast(DESCRIZIONE as varchar)   as inquinante
    from source

)

select *
from renamed