with source as (

    select *
    from {{ source('aria', 'rqa_t_reti') }}

),

renamed as (

    select
        cast(T_RETE as varchar)         as t_rete,
        cast(DESCR_T_RETE as varchar)   as descr_t_rete
    from source

)

select *
from renamed