with sum_of_items_per_month as (
	                               select sum( "order_item".amount )                  "sum_items"

		                                , extract( year from "order".created_at )  as Year
		                                , extract( month from "order".created_at ) as Month
		                                , max( concat( extract( year from "order".created_at ) , '-'
		                               , extract( month from "order".created_at ) ) ) Calender_Month
	                               from fdw_shop_service."order"
		                                    left join  fdw_shop_service."order_item"
			                                               on "order_item".fk_order = "order".id_order
		                                    inner join prod_info_layer.customer_table_horeca_children_customers_classified
			                                               on "UUID" = customer_uuid
	                               where "order".created_at >= '01-01-2022'
									 and ("Registration Date" is not null or customer_invitation_date is not null)

	                               group by extract( month from "order".created_at )
	                                      , extract( year from "order".created_at )
	                               order by extract( year from "order".created_at ) asc
	                                      , extract( year from "order".created_at )
                               )

select Calender_Month
	 , sum_items / (
		               select count( distinct id_customer )
		               from prod_info_layer.customer_table_horeca_children_customers_classified
					   where "Registration Date" is not null or customer_invitation_date is not null
	               ) as "average_number_of_items_per_Customer"
from sum_of_items_per_month

order by sum_of_items_per_month.Month, sum_of_items_per_month.Year asc