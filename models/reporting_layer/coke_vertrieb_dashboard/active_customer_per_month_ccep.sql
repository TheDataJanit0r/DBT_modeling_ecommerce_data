with active_customers_current_week_t_0 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text




		where user_uuid is not null and "Coke Kunde" = true--"order".created_at <= now( ) - '0 week':: interval
		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_0 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , max( concat( extract( 'month' from NOW( ) - '0 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '0 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ))                     "month"
			                      , extract( 'year' from NOW( ) )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_0
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_0."last order" )
		                                                    from active_customers_current_week_t_0
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_0."last order" )
			                                   from active_customers_current_week_t_0
		                                   )
	                         --   --   --   --  group by holding
                         )

   , active_customers_current_week_t_1 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text




		where "order".created_at <= now( ) - '1 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true
		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_1 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , max( concat( extract( 'month' from NOW( ) - '1 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '1 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '1 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '1 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_1
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_1."last order" )
		                                                    from active_customers_current_week_t_1
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_1."last order" )
			                                   from active_customers_current_week_t_1
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_2 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text



		where "order".created_at <= now( ) - '2 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_2 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , max( concat( extract( 'month' from NOW( ) - '2 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '2 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '2 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_2
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_2."last order" )
		                                                    from active_customers_current_week_t_2
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_2."last order" )
			                                   from active_customers_current_week_t_2
		                                   )

	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_3 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text


		where "order".created_at <= now( ) - '3 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_3 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , max( concat( extract( 'month' from NOW( ) - '3 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '3 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '3 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_3
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_3."last order" )
		                                                    from active_customers_current_week_t_3
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_3."last order" )
			                                   from active_customers_current_week_t_3
		                                   )

	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_4 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text



		where "order".created_at <= now( ) - '4 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_4 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '4 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '4 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '4 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding


	                         FROM active_customers_current_week_t_4
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_4."last order" )
		                                                    from active_customers_current_week_t_4
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_4."last order" )
			                                   from active_customers_current_week_t_4
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_5 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text



		where "order".created_at <= now( ) - '5 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_5 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '5 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '5 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '5 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_5
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_5."last order" )
		                                                    from active_customers_current_week_t_5
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_5."last order" )
			                                   from active_customers_current_week_t_5
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_6 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text


		where "order".created_at <= now( ) - '6 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_6 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '6 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '6 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '6 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_6
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_6."last order" )
		                                                    from active_customers_current_week_t_6
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_6."last order" )
			                                   from active_customers_current_week_t_6
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_7 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text



		where "order".created_at <= now( ) - '7 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_7 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '7 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '7 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '7 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_7
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_7."last order" )
		                                                    from active_customers_current_week_t_7
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_7."last order" )
			                                   from active_customers_current_week_t_7
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_8 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text

			     left join fdw_customer_service.customer_has_parent
			               on customer_table_horeca_children_customers_classified.id_customer =
			                  customer_has_parent.fk_customer


		where "order".created_at <= now( ) - '8 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_8 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '8 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '8 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '8 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_8
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_8."last order" )
		                                                    from active_customers_current_week_t_8
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_8."last order" )
			                                   from active_customers_current_week_t_8
		                                   )
	                         --   --   --   --  group by holding
                         )
   , active_customers_current_week_t_9 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join prod_info_layer.customer_table_horeca_children_customers_classified
			               on customer_table_horeca_children_customers_classified."UUID"::text = "order".customer_uuid::text



		where "order".created_at <= now( ) - '9 month':: interval and user_uuid is not null
		  and "Coke Kunde" = true

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , current_week_t_9 as (


	                         SELECT count( distinct id_customer )                                 AS "count"
			                      , 'KIP'                                                                   "app"
-- 			                      , min( concat( extract( 'month' from NOW( ) - '9 month':: interval ), '-',
-- 			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) ) as "Week of the Year"
			                      , extract( 'month' from NOW( ) - '9 month':: interval )                     "Week"
			                      , extract( 'year' from NOW( ) - '9 month':: interval )                     "Year"
			                      -- , case when holding is null then 'rest' else holding end               as holding

	                         FROM active_customers_current_week_t_9
	                              -- ,  left join prod_raw_layer.holdings using (merchant_key)
	                         where "last order" between (
		                                                    select max( active_customers_current_week_t_9."last order" )
		                                                    from active_customers_current_week_t_9
	                                                    ) - '28 day':: interval
		                               and (
			                                   select max( active_customers_current_week_t_9."last order" )
			                                   from active_customers_current_week_t_9
		                                   )
	                         --   --   --   --  group by holding
                         )
   , final as (
	              select *
	              from current_week_t_0
	              union all
	              select *
	              from current_week_t_1
	              union all
	              select *
	              from current_week_t_2
	              union all
	              select *
	              from current_week_t_3
	              union all
	              select *
	              from current_week_t_4
	              union all
	              select *
	              from current_week_t_5
	              union all
	              select *
	              from current_week_t_6
	              union all
	              select *
	              from current_week_t_7
	              union all
	              select *
	              from current_week_t_8
	              union all
	              select *
	              from current_week_t_9
              )
select *,concat(month, '-', "Year" )  as "Month of the Year"
from final
order by  "Year",month asc