{{
    config(
        enabled=true,
        severity='error'
    )
}}

with data as (
  
  select count(*) as amount
  from {{ref('pim_catalog_product_model')}}
  
),

validation as (

  select *
  from data
  where amount < (select rows::int from sheet_loader.row_test_limits where table_name = 'pim_catalog_product_model') 

)

select * from validation
