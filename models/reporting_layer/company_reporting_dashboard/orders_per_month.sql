with shop as (
	             SELECT count( id_order ) AS                         "count"
		              , max( concat( extract( 'month' from created_at ), '-',
		                             extract( 'year' from created_at ) ) ) "calender week"
		              , 'KIP'                                      "App"
		              , max( extract( 'month' from created_at ) )           "month"
		              , max( extract( 'year' from created_at ) )           "year"
	             FROM fdw_shop_service."order" --orders of shop
	             where (created_at between now( ) - '10 month'::interval and now( )::date + '7 day'::interval) and user_uuid is not null

	             group by concat( extract( 'month' from created_at ), '-', extract( 'year' from created_at ) )
	             order by "year", "month" asc
             )



   , shop_cleaned as (
	                     select count, "calender week", "App", year, month
	                     from shop

	                     order by "year", "month" asc
 	                     offset 1
                     )


   , final as (
	              select count, "calender week", "App", year, month
	              from shop_cleaned
	              where "calender week" not like '%52-2022%'

              )
select *
from final
