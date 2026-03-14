with source as (

    select *
    from {{ source('aria', 'rqa_metodi') }}

),

renamed as (

    select
        cast(COD_METODO as varchar)   as cod_metodo,
        cast(DESCRIZIONE as varchar)  as metodo
    from source

)

select *
from renamed