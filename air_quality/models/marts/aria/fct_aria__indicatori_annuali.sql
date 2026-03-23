{{ config(materialized='table') }}

with o3 as (

    select *
    from {{ ref('int_aria__indicatori_annuali_o3') }}

),

no2 as (

    select *
    from {{ ref('int_aria__indicatori_annuali_no2') }}

),

final as (

    select * from o3
    union all
    select * from no2

)

select *
from final