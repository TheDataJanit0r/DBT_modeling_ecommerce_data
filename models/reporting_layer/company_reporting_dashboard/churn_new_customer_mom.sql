with active_customers_current_week_t_9 as
	(
		select min( "order".created_at ) "first order"


			 , 'KIP'                     "App"
			 , customer_uuid

		from fdw_shop_service."order"

			     left join fdw_customer_service.merchant
				               on "order".merchant_uuid::text = merchant.uuid::text



		group by "order".customer_uuid


		order by min( "order".created_at ) asc
	)
   , list_customers_t_9 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"
	                           , "first order"


	                           FROM active_customers_current_week_t_9
	                           where "first order" >= date_trunc('month', current_date - interval '9' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '8' month)
	                           -- ,  left join prod_raw_layer.holdings using (merchant_key)


                           )







   , list_customers_t_8 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                           where "first order" >= date_trunc('month', current_date - interval '8' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '7' month)


                           )



   , list_customers_t_7 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                          where "first order" >= date_trunc('month', current_date - interval '7' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '6' month)


                           )



   , list_customers_t_6 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                         where "first order" >= date_trunc('month', current_date - interval '6' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '5' month)


                           )



   , list_customers_t_5 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                          where "first order" >= date_trunc('month', current_date - interval '5' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '4' month)


                           )


   , list_customers_t_4 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                           where "first order" >= date_trunc('month', current_date - interval '4' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '3' month)


                           )



   , list_customers_t_3 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                          where "first order" >= date_trunc('month', current_date - interval '3' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '2' month)


                           )



   , list_customers_t_2 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                          where "first order" >= date_trunc('month', current_date - interval '2' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '1' month)


                           )



   , list_customers_t_1 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                           where "first order" >= date_trunc('month', current_date - interval '1' month)
			                  and  "first order" <= date_trunc('month', current_date - interval '0' month)


                           )



   , list_customers_t_0 as (


	                           SELECT distinct customer_uuid AS "id_customer"
	                                         , 'KIP'            "app"


	                           FROM active_customers_current_week_t_9
	                          where "first order" >= date_trunc('month', current_date - interval '0' month)



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
		                                                 select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '9 month':: interval
				                                            and "order".created_at <= NOW( ) - '8 month':: interval
				                                            and user_uuid is not null
	                                                  )                                                    active_customers
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )


   , churn_from_t_9_to_t_7 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '8 month':: interval
				                                            and "order".created_at <= NOW( ) - '7 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "ending month"
	                              from list_customers_t_9

                              )
   , churn_from_t_9_to_t_6 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '7 month':: interval
				                                            and "order".created_at <= NOW( ) - '6 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_5 as (
	                              select count( * ) - (
		                                                 select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '6 month':: interval
				                                            and "order".created_at <= NOW( ) - '5 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )
   , churn_from_t_9_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '5 month':: interval
				                                            and "order".created_at <= NOW( ) - '4 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )
   , churn_from_t_9_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '9 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '9 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '9 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_9
                              )

   , churn_from_t_9_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_9
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_9.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '8 month':: interval
				                                            and "order".created_at <= NOW( ) - '7 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "ending month"
	                              from list_customers_t_8

                              )

   , churn_from_t_8_to_t_6 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '7 month':: interval
				                                            and "order".created_at <= NOW( ) - '6 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_8

                              )
   , churn_from_t_8_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '6 month':: interval
				                                            and "order".created_at <= NOW( ) - '5 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )

   , churn_from_t_8_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '5 month':: interval
				                                            and "order".created_at <= NOW( ) - '4 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )
   , churn_from_t_8_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )

   , churn_from_t_8_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )
   , churn_from_t_8_to_t_1 as (
	                              select count( * ) - (
		                                                 select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '8 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '8 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '8 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_8
                              )


   , churn_from_t_8_to_t_0 as (
	                              select count( * ) - (
		                                                 select count( distinct id_customer )
		                                                  from list_customers_t_8
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_8.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '7 month':: interval
				                                            and "order".created_at <= NOW( ) - '6 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "ending month"
	                              from list_customers_t_7

                              )
   , churn_from_t_7_to_t_5 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '6 month':: interval
				                                            and "order".created_at <= NOW( ) - '5 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '5 month':: interval
				                                            and "order".created_at <= NOW( ) - '4 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )
   , churn_from_t_7_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )
   , churn_from_t_7_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '7 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '7 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '7 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_7
                              )

   , churn_from_t_7_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_7
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_7.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '6 month':: interval
				                                            and "order".created_at <= NOW( ) - '5 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_4 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '5 month':: interval
				                                            and "order".created_at <= NOW( ) - '4 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )
   , churn_from_t_6_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )
   , churn_from_t_6_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '6 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '6 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '6 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_6
                              )

   , churn_from_t_6_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_6
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_6.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_5
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_5.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '5 month':: interval
				                                            and "order".created_at <= NOW( ) - '4 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )
   , churn_from_t_5_to_t_3 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_5
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_5.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )

   , churn_from_t_5_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_5
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_5.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )
   , churn_from_t_5_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_5
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_5.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '5 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '5 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '5 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_5
                              )

   , churn_from_t_5_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_5
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_5.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_4
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_4.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '4 month':: interval
				                                            and "order".created_at <= NOW( ) - '3 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )

   , churn_from_t_4_to_t_2 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_4
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_4.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )
   , churn_from_t_4_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_4
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_4.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '4 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '4 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '4 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_4
                              )

   , churn_from_t_4_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_4
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_4.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_3
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_3.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '3 month':: interval
				                                            and "order".created_at <= NOW( ) - '2 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "ending month"
	                              from list_customers_t_3
                              )
   , churn_from_t_3_to_t_1 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_3
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_3.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '3 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '3 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '3 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_3
                              )

   , churn_from_t_3_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_3
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_3.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
	                              from list_customers_t_2

                              )
   , churn_from_t_2_to_t_1 as (
	                              select count( * ) - (
		                                                 select count( distinct id_customer )
		                                                  from list_customers_t_2
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_2.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '2 month':: interval
				                                            and "order".created_at <= NOW( ) - '1 month':: interval
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) - '1 month':: interval )             "ending month"
	                              from list_customers_t_2
                              )

   , churn_from_t_2_to_t_0 as (
	                              select count( * ) - (
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_2
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_2.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
	                                                  )
			                           , concat( extract( 'month' from NOW( ) - '2 month':: interval ) , '-' ,
			                                     extract( 'year' from NOW( ) - '2 month':: interval ) ) as "month of the Year"
			                           , extract( 'month' from NOW( ) - '2 month':: interval )             "starting month"
			                           , extract( 'month' from NOW( ) )                                    "ending month"
	                              from list_customers_t_2
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
		                                                  select count( distinct id_customer )
		                                                  from list_customers_t_1
			                                                       inner join fdw_shop_service."order"
				                                                                  on list_customers_t_1.id_customer = "order".customer_uuid
		                                                  where "order".created_at >= NOW( ) - '1 month':: interval
				                                            and "order".created_at <= NOW( )
				                                            and user_uuid is not null
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
	                       from churn_from_t_3_to_t_2
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
from final_to_write


