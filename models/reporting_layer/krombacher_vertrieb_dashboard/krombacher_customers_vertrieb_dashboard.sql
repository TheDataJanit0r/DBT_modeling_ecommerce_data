with krombacher_customers as (
	                             select *
	                             from {{ref('customer_table_horeca_children_customers_classified')}} 
	                             where "Krombacher Kunde" = true
                             )


   , shop_krombacher_orders as (
	                               select id_order_item
			                            , id_order
			                            , "order".created_at as "order creation date"
			                            , customer_uuid      as "Company Key"
			                            , customer_uuid
			                            , amount

	                               from fdw_shop_service.order_item
		                                    left join fdw_shop_service."order"
			                                              on "order".id_order =
			                                                 order_item.fk_order
		                                    left join fdw_shop_service."product"
			                                              on product.id_product =
			                                                 order_item.fk_product
	                               where order_item.product_name::text like
	                                     '%rombach%'
			                          or order_item.product_name::text like
			                             '%chwep%'
			                          or order_item.product_name::text like
			                             '%urgsteinfurter%'
			                          or order_item.product_name::text like
			                             '%henania%'

			                          or order_item.product_name::text like
			                             '%itamalz%'

			                          or order_item.product_name::text like
			                             '%olinck%'

                               )

   , krombacher_items as (
	                         select count( id_order_item ) "Krombacher items"
			                      , id_order
			                      , customer_uuid
	                         from shop_krombacher_orders

	                         group by id_order
	                                , customer_uuid


                         )


   , total_orders as (
	                     select max( amount ) "total item Count", id_order
	                     from shop_krombacher_orders
	                     group by id_order

                     )

   , percentage_of_krombacher_items as (
	                                       select krombacher_items.customer_uuid
			                                    , krombacher_items."Krombacher items"
			                                    , "total item Count"

			                                    , round(
			                                       ( krombacher_items."Krombacher items"::numeric /
			                                         case
				                                         when "total item Count"::numeric = 0
					                                         then null
				                                         else "total item Count"::numeric
			                                         end )::numeric
		                                       , 2 )::numeric "percentage of krombacher items"
	                                       from total_orders
		                                            left join krombacher_items
			                                                      on total_orders.id_order = krombacher_items.id_order


                                       )

   , grouped_on_company_level as (
	                                 select round( avg( "percentage of krombacher items" ) , 2 ) "percentage of krombacher items"
			                              , customer_uuid
	                                 from percentage_of_krombacher_items as a
	                                 where a.customer_uuid::text in (
		                                                                select "UUID"::text
		                                                                from krombacher_customers

	                                                                )
	                                 group by customer_uuid


                                 )

---------------------- i stopped checking here
   , first_last_order_krombacher as (
	                                    select min( "order creation date" ) "first order Krombacher"
			                                 , customer_uuid
			                                 , max( "order creation date" ) "last order Krombacher"
	                                    from shop_krombacher_orders
	                                    where customer_uuid::text in (
		                                                                 select "UUID"::text
		                                                                 from krombacher_customers
	                                                                 )
	                                    group by customer_uuid
                                    )

   , order_count_shop as (
	                         select count( id_order ) "order count", customer_uuid
	                         from fdw_shop_service."order"
	                         group by customer_uuid
                         )

   , shop_customer_order_consolidated as (
	                                         select krombacher_customers.*
			                                      , first."first order Krombacher"
			                                      , "last order Krombacher"
			                                      , percent."percentage of krombacher items"
			                                      , order_count_shop."order count"        as "order count"
			                                      , concat( krombacher_customers."Street" , '-' ,
			                                                krombacher_customers."City" ) as "address"
			                                      , merchant.key                          as merchant_key

	                                         from krombacher_customers
		                                              left join first_last_order_krombacher as first
			                                                        on krombacher_customers."UUID"::text =
			                                                           first.customer_uuid::text
		                                              left join grouped_on_company_level    as percent
			                                                        on krombacher_customers."UUID"::text =
			                                                           percent.customer_uuid::text
		                                              left join order_count_shop
			                                                        on order_count_shop.customer_uuid::text =
			                                                           krombacher_customers."UUID"::text
		                                              left join fdw_customer_service.customer_has_merchant
			                                                        on id_customer = customer_has_merchant.fk_customer
		                                              left join fdw_customer_service.merchant
			                                                        on merchant.id_merchant
			                                                        = customer_has_merchant.fk_merchant


                                         )

   , filtered_shop_table as (
	                            select "Customer Name"
			                         , "Customer Status"
			                         , "UUID"

			                         , "Customer Email"
			                         , "Last Order Created"
			                         , extract( 'day' from now( ) - "Last Order Created"::date ) "Tagen seit der Letze Aktivitaet"
			                         , "Registration Date"
			                         , "order count"
			                         , merchant_key
			                         , 'KIP' as                                                  "App"
			                         , "id_customer"
			                         , address
			                         , "Zip Code"
			                         , "City"
			                         , "percentage of krombacher items"
			                         , "first order Krombacher"
			                         , "last order Krombacher"


	                            from shop_customer_order_consolidated

                            )


   , final_table_without_gedatID as (
	                                    select *
	                                    from filtered_shop_table

                                    )


   , match_gedat as (
	                    select model."Customer Name"                        "Customer Name"
			                 , model."Customer Status"                      "Customer Status"
			                 , model."UUID"::text                           "UUID"
			                 , model."Customer Email"                       "Customer Email"
			                 , model."Last Order Created"                   "Last Order Created"
			                 , model."Tagen seit der Letze Aktivitaet"      "Tagen seit der Letze Aktivitaet"
			                 , model."Registration Date"                    "Registration Date"
			                 , model."order count"                          "order count"
			                 , model.merchant_key                           merchant_key
			                 , model."App"                                  "App"
			                 , model.id_customer as   id_customer
			                 , model.address                                address
			                 , model."Zip Code"                             "Zip Code"
			                 , model."City"                                 "City"
			                 , model."percentage of krombacher items"       "percentage of krombacher items"
			                 , model."first order Krombacher"               "first order Krombacher"
			                 , model."last order Krombacher"                "last order Krombacher"
			                 , gedat."gedat_id"                             "gedat_id"
			                 , krombacher_matched_stammdaten."Objektnummer" "Objektnummer"
	                    from final_table_without_gedatID              as model
		                         left join sheet_loader.gedat_results as gedat
			                                   on model.address = gedat."gedat_strasse_1" or
			                                      model."Customer Name" = gedat."gedat_name_1"

		                         left join prod_info_layer.krombacher_matched_stammdaten
			                                   on model.id_customer = krombacher_matched_stammdaten.id_customer



                    )



select merchant_key                      as GFGH
	 , match_gedat."Customer Name"          "Kundenname"
	 , id_customer                          "Kollex ID"
	 , "Objektnummer"                       krombacher_objektnummer
	 , "Customer Status"
	 , address
	 , "Zip Code"
	 , "City"
	 , "Last Order Created"
	 , "Tagen seit der Letze Aktivitaet" as "Tage seit der letze Aktivitaet"
	 , "Registration Date"
	 , "App"
	 , id_customer
	 , gedat_id
	 , "order count"

	 , "first order Krombacher"
	 , "last order Krombacher"
	 , "percentage of krombacher items"

from match_gedat