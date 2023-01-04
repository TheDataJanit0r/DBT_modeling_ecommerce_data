with Bitburger_customers as (
	                             select id_customer
		                              , "Registration Date"
		                              , extract( 'month' from "Registration Date"::date ) "month"
		                              , extract( 'year' from "Registration Date"::date ) "year"
	                             from {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
	                             where "Bitburger Kunde" = true
		                           and "Registration Date" is not null
                             )


   , final as (
	              select count( id_customer), max( concat( month, '-', year ) ) "calender week",year,month
	              from Bitburger_customers

	              where concat( month, '-', year ) <> '52-2022'
			        and concat( month, '-', year ) <> '53-2021'
	              group by year, month
	              order by year, month asc


              )
select *
from final
offset(select case when count(*)>10 then count(*)-10 end from final)

