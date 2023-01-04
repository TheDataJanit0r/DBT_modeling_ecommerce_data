with number_of_orders_per_month as (
	                                   select count( distinct id_order )               as count_per_month
		                                    , max( concat( extract( year from "order".created_at ) , '-'
		                                   , extract( month from "order".created_at ) ) ) Calender_Month
		                                    , extract( year from "order".created_at )  as Year
		                                    , extract( month from "order".created_at ) as Month
	                                   from fdw_shop_service."order"
	                                        inner join prod_info_layer.customer_table_horeca_children_customers_classified
	                                                on "UUID" = customer_uuid
	                                   where created_at >='01-01-2022' and "Coke Kunde" = 'true'
	                                   group by extract( month from "order".created_at )
	                                          , extract( year from "order".created_at )

                                   )
   , sum_of_items_per_month as (
	                               select sum( "order_item".amount )                  "sum_items"

			                            , extract( year from "order".created_at )  as Year
			                            , extract( month from "order".created_at ) as Month
			                            , max( concat( extract( year from "order".created_at ) , '-'
		                               , extract( month from "order".created_at ) ) ) Calender_Week
	                               from fdw_shop_service."order"
		                                    left join fdw_shop_service."order_item"
			                                              on "order_item".fk_order = "order".id_order
											 inner join prod_info_layer.customer_table_horeca_children_customers_classified
	                                                on "UUID" = customer_uuid
	                               where "order".created_at >= '01-01-2022'  and "Coke Kunde" = 'true'

	                               group by extract( month from "order".created_at )
	                                      , extract( year from "order".created_at )
	                               order by extract( year from "order".created_at ) asc
	                                      , extract( year from "order".created_at )
                               )

select Calender_Month, sum_items / count_per_month as "average_number_of_items_per_basket"
from number_of_orders_per_month
	     left join sum_of_items_per_month on number_of_orders_per_month.Month = sum_of_items_per_month.Month
	and number_of_orders_per_month.Year = sum_of_items_per_month.Year
	order by number_of_orders_per_month.Month,number_of_orders_per_month.Year asc