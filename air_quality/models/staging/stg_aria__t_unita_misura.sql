with source as (

    select *
    from {{ source('aria', 't_unita_misura') }}

),

renamed as (

    select
        cast(COD_MIS as varchar)    as cod_mis,
        cast(SIGLA_MIS as varchar)  as sigla_mis
    from source

)

select *
from renamed