with order_count_shop as (
	                         select count( id_order ) as                                            count

		                          , min( gedat_id )                                                 gedat_id
		                          , customer_table_horeca_children_customers_classified.id_customer id_customer


	                         from prod_info_layer.customer_table_horeca_children_customers_classified
		                              left join fdw_shop_service."order"
			                                        on "order".customer_uuid =
			                                           customer_table_horeca_children_customers_classified."UUID"

		                              left join prod_info_layer.gedat_matched_jvp_classified_customers
			                                        on "order".customer_uuid::text =
			                                           gedat_matched_jvp_classified_customers."UUID"::text
	                         where customer_table_horeca_children_customers_classified."Coke Kunde" = TRUE
		                       and order_uuid is not null
	                         group by customer_table_horeca_children_customers_classified.id_customer

                         )


select distinct on (classified.id_customer,coke_matched_stammdaten."Gsheet/Stammdaten Stammnummer") coke_matched_stammdaten."Gsheet/Stammdaten Stammnummer" as "Stammnummer"
                                                                                                  , coke_matched_stammdaten."Personalnummer Coke Mitarbeiter"  "Personalnummer"
                                                                                                  , classified.id_customer ::text                           as "kx_id"
                                                                                                  , classified."Customer Name"                              as "kundenname"
                                                                                                  , "Customer Status"                                       as "Status"
                                                                                                  , classified."Street"                                     as address
                                                                                                  , classified."Zip Code"
                                                                                                  , classified."City"

                                                                                                  , "Last Order Created"

                                                                                                  , gedat_id                                                as "GEDAT_ID"
                                                                                                  , order_count_shop.count                                     "order_count"
                                                                                                  , classified."Customer Email"
                                                                                                  , classified."Status Last Supplier Request"
                                                                                                  , classified."GFGH"
                                                                                                  , address.id_address
                                                                                                  , ccep_stammdaten_merchants."CCEP Nr."
                                                                                                  , concat( address.street , ' ' , address.house_number )      street
                                                                                                  , address.zip
                                                                                                  , address.city
                                                                                                  , address.country


from prod_info_layer.customer_table_horeca_children_customers_classified as "classified"
	     left join order_count_shop on classified.id_customer = order_count_shop.id_customer
	     left join prod_info_layer.coke_matched_stammdaten
		               on coke_matched_stammdaten."Kollex Customer ID" = classified.id_customer
	     left join fdw_customer_service.merchant
		               on "GFGH" = merchant.key
	     left join fdw_customer_service.address
		               on merchant.fk_address = address.id_address
	     left join prod_info_layer.ccep_stammdaten_merchants
		               on ccep_stammdaten_merchants.key = "GFGH"


where classified."Coke Kunde" = true
  and "Registration Date" is not null
  and "Customer Email" not like '%kollex%'
  and "Customer Email" not like '%test%'





