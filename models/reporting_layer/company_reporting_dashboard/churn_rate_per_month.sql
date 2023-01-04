with active_customers_current_week_t_9 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '9 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_9 as (


	                           SELECT distinct id_customer AS "id_customer"
	                                         , 'KIP'          "app"


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

                           )


   , count_customers_t_9 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '9 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '9 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '9 month':: interval )                     "Year"


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


   , active_customers_current_week_t_8 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '8 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_8 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_8
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_8."last order" )
		                                                      from active_customers_current_week_t_8
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_8."last order" )
			                                     from active_customers_current_week_t_8
		                                     )

                           )

   , count_customers_t_8 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '8 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '8 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '8 month':: interval )                     "Year"


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


   , active_customers_current_week_t_7 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '7 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_7 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_7
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_7."last order" )
		                                                      from active_customers_current_week_t_7
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_7."last order" )
			                                     from active_customers_current_week_t_7
		                                     )

                           )

   , count_customers_t_7 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '7 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '7 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '7 month':: interval )                     "Year"


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

                            )
   , active_customers_current_week_t_6 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '6 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_6 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_6
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_6."last order" )
		                                                      from active_customers_current_week_t_6
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_6."last order" )
			                                     from active_customers_current_week_t_6
		                                     )

                           )

   , count_customers_t_6 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '6 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '6 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '6 month':: interval )                     "Year"


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

                            )
   , active_customers_current_week_t_5 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '5 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_5 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_5
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_5."last order" )
		                                                      from active_customers_current_week_t_5
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_5."last order" )
			                                     from active_customers_current_week_t_5
		                                     )

                           )

   , count_customers_t_5 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '5 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '5 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '5 month':: interval )                     "Year"


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

                            )
   , active_customers_current_week_t_4 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '4 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_4 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_4
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_4."last order" )
		                                                      from active_customers_current_week_t_4
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_4."last order" )
			                                     from active_customers_current_week_t_4
		                                     )

                           )

   , count_customers_t_4 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '4 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '4 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '4 month':: interval )                     "Year"


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

                            )


   , active_customers_current_week_t_3 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '3 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_3 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_3
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_3."last order" )
		                                                      from active_customers_current_week_t_3
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_3."last order" )
			                                     from active_customers_current_week_t_3
		                                     )

                           )

   , count_customers_t_3 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '3 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '3 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '3 month':: interval )                     "Year"


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

                            )


   , active_customers_current_week_t_2 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '2 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_2 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_2
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_2."last order" )
		                                                      from active_customers_current_week_t_2
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_2."last order" )
			                                     from active_customers_current_week_t_2
		                                     )

                           )

   , count_customers_t_2 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '2 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '2 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '2 month':: interval )                     "Year"


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

                            )


   , active_customers_current_week_t_1 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where "order".created_at <= now( ) - '1 month':: interval
		  and user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_1 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_1
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_1."last order" )
		                                                      from active_customers_current_week_t_1
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_1."last order" )
			                                     from active_customers_current_week_t_1
		                                     )

                           )

   , count_customers_t_1 as (


	                            SELECT count( distinct id_customer )                                         AS "id_customer"
			                         , 'KIP'                                                                    "app"
			                         , max( concat( extract( 'month' from NOW( ) - '1 month':: interval ) , '-' ,
			                                        extract( 'year' from NOW( ) - '1 month':: interval ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) - '1 month':: interval )                    "month"
			                         , extract( 'year' from NOW( ) - '1 month':: interval )                     "Year"


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

                            )


   , active_customers_current_week_t_0 as
	(
		select max( "order".created_at ) "last order", id_customer, max( merchant.key::text ) as merchant_key
		from fdw_shop_service."order"
			     left join fdw_customer_service.merchant on "order".merchant_uuid = merchant.uuid
			     left join {{ref('customer_table_horeca_children_customers_classified')}}  as customer_table_horeca_children_customers_classified
				               on customer_table_horeca_children_customers_classified."UUID"::text =
				                  "order".customer_uuid::text


		where user_uuid is not null

		group by customer_table_horeca_children_customers_classified.id_customer
	)
   , list_customers_t_0 as (


	                           SELECT distinct id_customer AS "id_customer"


	                           FROM active_customers_current_week_t_0
	                           where "last order" between (
		                                                      select max( active_customers_current_week_t_0."last order" )
		                                                      from active_customers_current_week_t_0
	                                                      ) - '28 day':: interval
		                                 and (
			                                     select max( active_customers_current_week_t_0."last order" )
			                                     from active_customers_current_week_t_0
		                                     )

                           )

   , count_customers_t_0 as (


	                            SELECT count( distinct id_customer )                  AS "id_customer"
			                         , 'KIP'                                             "app"
			                         , max( concat( extract( 'month' from NOW( ) ) , '-' ,
			                                        extract( 'year' from NOW( ) ) ) ) as "month of the Year"
			                         , extract( 'month' from NOW( ) )                    "month"
			                         , extract( 'year' from NOW( ) )                     "Year"


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

                            )
   , churn_from_t_9_to_t_9 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )

   , churn_from_t_9_to_t_8 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_8
				                                                                 on list_customers_t_9.id_customer = list_customers_t_8.id_customer
		                                                  where list_customers_t_8.id_customer is null
	                                                  )                                                    active_customers
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )

   , churn_from_t_9_to_t_7 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_7
				                                                                 on list_customers_t_9.id_customer = list_customers_t_7.id_customer
		                                                  where list_customers_t_7.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )
   , churn_from_t_9_to_t_6 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_6
				                                                                 on list_customers_t_9.id_customer = list_customers_t_6.id_customer
		                                                  where list_customers_t_6.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_5
				                                                                 on list_customers_t_9.id_customer = list_customers_t_5.id_customer
		                                                  where list_customers_t_5.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )
   , churn_from_t_9_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_4
				                                                                 on list_customers_t_9.id_customer = list_customers_t_4.id_customer
		                                                  where list_customers_t_4.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_9.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )
   , churn_from_t_9_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_9.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_9.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_9
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_9.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_9
                              )
   , churn_from_t_8_to_t_8 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "ending month"
	                              from list_customers_t_8

                              )
   , churn_from_t_8_to_t_7 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_7
				                                                                 on list_customers_t_8.id_customer = list_customers_t_7.id_customer
		                                                  where list_customers_t_7.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "ending month"
	                              from list_customers_t_8

                              )

   , churn_from_t_8_to_t_6 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_6
				                                                                 on list_customers_t_8.id_customer = list_customers_t_6.id_customer
		                                                  where list_customers_t_6.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_8

                              )
   , churn_from_t_8_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_5
				                                                                 on list_customers_t_8.id_customer = list_customers_t_5.id_customer
		                                                  where list_customers_t_5.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )

   , churn_from_t_8_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_4
				                                                                 on list_customers_t_8.id_customer = list_customers_t_4.id_customer
		                                                  where list_customers_t_4.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )
   , churn_from_t_8_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_8.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )

   , churn_from_t_8_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_8.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )
   , churn_from_t_8_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_8.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )


   , churn_from_t_8_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_8
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_8.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_8
                              )
   , churn_from_t_7_to_t_7 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "ending month"
	                              from list_customers_t_7

                              )

   , churn_from_t_7_to_t_6 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_6
				                                                                 on list_customers_t_7.id_customer = list_customers_t_6.id_customer
		                                                  where list_customers_t_6.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_7

                              )
   , churn_from_t_7_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_5
				                                                                 on list_customers_t_7.id_customer = list_customers_t_5.id_customer
		                                                  where list_customers_t_5.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_4
				                                                                 on list_customers_t_7.id_customer = list_customers_t_4.id_customer
		                                                  where list_customers_t_4.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )
   , churn_from_t_7_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_7.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_7.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )
   , churn_from_t_7_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_7.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_7
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_7.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_7
                              )
   , churn_from_t_6_to_t_6 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_6

                              )
   , churn_from_t_6_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_5
				                                                                 on list_customers_t_6.id_customer = list_customers_t_5.id_customer
		                                                  where list_customers_t_5.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_4
				                                                                 on list_customers_t_6.id_customer = list_customers_t_4.id_customer
		                                                  where list_customers_t_4.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )
   , churn_from_t_6_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_6.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_6.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )
   , churn_from_t_6_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_6.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_6
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_6.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_6
                              )
   , churn_from_t_5_to_t_5 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_5

                              )
   , churn_from_t_5_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_5
			                                                       left join list_customers_t_4
				                                                                 on list_customers_t_5.id_customer = list_customers_t_4.id_customer
		                                                  where list_customers_t_4.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )
   , churn_from_t_5_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_5
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_5.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )

   , churn_from_t_5_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_5
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_5.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )
   , churn_from_t_5_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_5
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_5.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )

   , churn_from_t_5_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_5
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_5.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_5
                              )
   , churn_from_t_4_to_t_4 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_4

                              )
   , churn_from_t_4_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_4
			                                                       left join list_customers_t_3
				                                                                 on list_customers_t_4.id_customer = list_customers_t_3.id_customer
		                                                  where list_customers_t_3.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )

   , churn_from_t_4_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_4
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_4.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )
   , churn_from_t_4_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_4
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_4.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )

   , churn_from_t_4_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_4
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_4.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_4
                              )
   , churn_from_t_3_to_t_3 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_3

                              )
   , churn_from_t_3_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_3
			                                                       left join list_customers_t_2
				                                                                 on list_customers_t_3.id_customer = list_customers_t_2.id_customer
		                                                  where list_customers_t_2.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_3
                              )
   , churn_from_t_3_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_3
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_3.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_3
                              )

   , churn_from_t_3_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_3
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_3.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_3
                              )
   , churn_from_t_2_to_t_2 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )
   , churn_from_t_2_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_2
			                                                       left join list_customers_t_1
				                                                                 on list_customers_t_2.id_customer = list_customers_t_1.id_customer
		                                                  where list_customers_t_1.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_2
                              )

   , churn_from_t_2_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_2
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_2.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_3
                              )
   , churn_from_t_1_to_t_1 as (
	                              select count( * )                                                        active_customers
			                           , concat( extract( 'month' from NOW( ) - '1 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_1

                              )

   , churn_from_t_1_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( * )
		                                                  from list_customers_t_1
			                                                       left join list_customers_t_0
				                                                                 on list_customers_t_1.id_customer = list_customers_t_0.id_customer
		                                                  where list_customers_t_0.id_customer is null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '1 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '1 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_1
                              )

   , churn_from_t_0_to_t_0 as (
	                              select count( * )                                 active_customers
			                           , concat( extract( 'month' from NOW( ) ) , '-' ,
			                                     extract( 'year' from NOW( ) ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) )             "starting month"
			                           , extract( 'month' from NOW( ) )             "ending month"
	                              from list_customers_t_0

                              )

   , final_to_write as (
	                       select *
	                       from churn_from_t_9_to_t_9
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_8
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_7
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_6
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_5
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_4
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_9_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_8
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_7
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_6
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_5
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_4
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_8_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_7
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_6
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_5
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_4
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_7_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_6
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_5
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_4
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_6_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_5
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_4

	                       union all
	                       select *
	                       from churn_from_t_5_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_4_to_t_4
	                       union all
	                       select *
	                       from churn_from_t_4_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_4_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_4_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_4_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_3_to_t_3
	                       union all
	                       select *
	                       from churn_from_t_5_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_3_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_3_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_2_to_t_2
	                       union all
	                       select *
	                       from churn_from_t_2_to_t_1
	                       union all

	                       select *
	                       from churn_from_t_2_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_1_to_t_1
	                       union all
	                       select *
	                       from churn_from_t_1_to_t_0
	                       union all
	                       select *
	                       from churn_from_t_0_to_t_0


                       )
select active_customers
	 , case
		   when "starting month" = 1
			   then 'January'
		   when "starting month" = 2
			   then 'February'
		   when "starting month" = 3
			   then 'March'
		   when "starting month" = 4
			   then 'April'
		   when "starting month" = 5
			   then 'May'
		   when "starting month" = 6
			   then 'June'
		   when "starting month" = 7
			   then 'July'
		   when "starting month" = 8
			   then 'August'
		   when "starting month" = 9
			   then 'September'
		   when "starting month" = 10
			   then 'October'
		   when "starting month" = 11
			   then 'November'
		   when "starting month" = 12
			   then 'December'
	   end as "starting month"
	 , case
		   when "ending month" = 1
			   then 'January'
		   when "ending month" = 2
			   then 'February'
		   when "ending month" = 3
			   then 'March'
		   when "ending month" = 4
			   then 'April'
		   when "ending month" = 5
			   then 'May'
		   when "ending month" = 6
			   then 'June'
		   when "ending month" = 7
			   then 'July'
		   when "ending month" = 8
			   then 'August'
		   when "ending month" = 9
			   then 'September'
		   when "ending month" = 10
			   then 'October'
		   when "ending month" = 11
			   then 'November'
		   when "ending month" = 12
			   then 'December'
	   end as "ending month"
-- into prod_info_layer.churn_customer_mom
from final_to_write
