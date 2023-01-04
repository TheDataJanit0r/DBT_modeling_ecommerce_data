with orders as (
	               select min( "order".created_at ) "first order"


		                , 'KIP'                     "App"
		                , customer_uuid

	               from fdw_shop_service."order"

		                    left join fdw_customer_service.merchant
			                              on "order".merchant_uuid::text = merchant.uuid::text
		                    left join {{ref('customer_table_horeca_children_customers_classified')}}  as cth
			                              on "order".customer_uuid = cth."UUID"

	               where "Coke Kunde" = 'true'


	               group by "order".customer_uuid


	               order by min( "order".created_at ) asc
               )
   , filter_orders as (
	                      select *
			                   , extract( 'year' from "first order" )                               "year"
			                   , extract( 'month' from "first order" )                               "month"

	                      from orders
	                      where "first order" >= now( ) - '10 month'::interval - '1 day'::interval
                      )


   , final as (

	              select count( distinct customer_uuid ) "count"
			           , concat( month , '-' , year )     "calender week"

			           , year
			           , month
	              from filter_orders
	              where concat( year , '-' , month ) <> '2022-52'

	              group by year, month

	              order by year, month
              )


select sum( "count" ), max( "calender week" ), year, month
from final

group by year, month
order by year, month