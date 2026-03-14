with source as (

    select *
    from {{ source('aria', 'rqa_t_stclass_eu') }}

),

renamed as (

    select
        cast(ID as varchar)       as station_cl_eu,
        cast(DESC_EU as varchar)  as desc_eu
    from source

)

select *
from renamed