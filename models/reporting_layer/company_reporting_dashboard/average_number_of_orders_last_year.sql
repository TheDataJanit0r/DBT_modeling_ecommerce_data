with number_of_orders_per_month as (


	                                   select count( id_order )                as count
		                                    , extract( month from created_at ) as month
		                                    , extract( year from created_at )  as year
	                                   from fdw_shop_service.order
	                                   where extract( year from created_at ) = extract( year from now() )-1
	                                   group by extract( month from created_at ), extract( year from created_at )
	                                   order by extract( year from created_at )  asc
	                                          , extract( month from created_at ) asc
                                   )

select  round(count::numeric/ (
	               select count( distinct id_customer ) from {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
               ),3)
	 , month
	 , year
from number_of_orders_per_month
