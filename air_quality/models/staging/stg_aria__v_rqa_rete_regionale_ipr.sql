with source as (

    select *
    from {{ source('aria', 'v_rqa_rete_regionale_ipr') }}

),

renamed as (

    select
        cast(cod_ubic as varchar) as cod_ubic,
        cast(cod_conf as integer) as cod_conf,
        cast(anno as integer) as anno
    from source

)

select *
from renamed
