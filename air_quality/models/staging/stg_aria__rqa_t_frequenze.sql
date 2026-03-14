with source as (

    select *
    from {{ source('aria', 'rqa_t_frequenze') }}

),

renamed as (

    select
        cast(COD_FREQ as varchar)    as cod_freq,
        cast(DESCR_FREQ as varchar)  as descr_freq
    from source

)

select *
from renamed