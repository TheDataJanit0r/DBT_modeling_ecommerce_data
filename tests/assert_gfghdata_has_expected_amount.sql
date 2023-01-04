with data as (
  
  select count(*) as amount
  from {{ref('gfgh_data')}}
  
),

validation as (

  select *
  from data
  where amount < (select rows::int from sheet_loader.row_test_limits where table_name = 'gfgh_data') 

)

select * from validation
