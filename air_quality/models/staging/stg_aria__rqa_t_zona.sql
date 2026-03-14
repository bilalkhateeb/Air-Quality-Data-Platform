with source as (

    select *
    from {{ source('aria', 'rqa_t_zona') }}

),

renamed as (

    select
        cast(COD_TZONA as varchar)  as cod_tzona,
        cast(TIPO_ZONA as varchar)  as tipo_zona
    from source

)

select *
from renamed