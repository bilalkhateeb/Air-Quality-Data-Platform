with source as (

    select *
    from {{ source('aria', 'rqa_dati_aria_cor') }}

),

cleaned as (

    select
        cast(COD_UBIC as varchar) as cod_ubic,
        cast(COD_CONF as integer) as cod_conf,
        cast(DATA_DA as timestamp) as data_da,
        cast(DATA_A as timestamp) as data_a,
        cast(VAL_PARAM as double) as val_param,
        cast(
            case
                when cast(COD_VALID as integer) > 1 then 1
                else cast(COD_VALID as integer)
            end as integer
        ) as cod_valid,
        cast(COD_VALIDAZ as varchar) as cod_validaz,
        cast(COD_VALIDAZ_REG as varchar) as cod_validaz_reg
    from source

),

final as (

    select
        cod_ubic,
        cod_conf,
        data_da,
        data_a,
        val_param,
        cod_valid,
        cod_validaz,
        cod_validaz_reg,
        extract(year from data_da) as anno
    from cleaned

)

select *
from final