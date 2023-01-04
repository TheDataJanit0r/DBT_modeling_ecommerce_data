select distinct on (classified.id_customer) coke_matched_stammdaten."Gsheet/Stammdaten Stammnummer" as "Stammnummer"
                                          , "Personalnummer Coke Mitarbeiter"                       as "Personalnummer"
                                          , classified.id_customer                                  as "kx_id"
                                          , classified."Customer Name"                              as "Kundenname"
                                          , classified."Customer Status"                            as "Status"
                                          , classified."Street"                                     as "Straße&Hnr."
                                          , classified."Zip Code"                                   as "PLZ"
                                          , classified."City"                                          "Stadt"
                                          , gedat_id                                                as "GEDAT_ID"
                                          , classified."Number Of Orders"                           as "order_count"
                                          , classified."Customer Email"
                                          , classified."Last Order Created"
                                          , classified."Status Last Supplier Request"
                                          , classified."GFGH"
                                          , ccep_stammdaten_merchants."CCEP Nr."                       "Stammnummer Lieferant"
                                          , concat( address.street , ' ' , address.house_number )   as "address_merchant"
                                          , address.city                                            as "city_merchant"
                                          , address.zip                                             as "plz_merchant"
                                          , address.country                                         as "country_merchant"
                                          , customer_has_merchant.updated_at                        as "merchant_activation_date"
                                          , coke_matched_stammdaten.token
                                          , classified."Registration Date"

from prod_info_layer.customer_table_horeca_children_customers_classified classified
	     left join prod_info_layer.ccep_stammdaten_merchants
		               on "GFGH" = ccep_stammdaten_merchants.key
	     left join fdw_customer_service.merchant
		               on "GFGH" = merchant.key
	     left join fdw_customer_service.address
		               on merchant.fk_address = address.id_address
	     left join fdw_customer_service.customer_has_merchant
		               on fk_customer = id_customer
	     left join prod_info_layer.coke_matched_stammdaten
		               on classified.id_customer =
		                  coke_matched_stammdaten."Kollex Customer ID"
	     left join prod_info_layer.gedat_matched_jvp_classifed_customers
		               on gedat_matched_jvp_classifed_customers.id_customer =
		                  classified.id_customer
where classified."Coke Kunde" = 'true'
  and ( classified."Registration Date" is not null or customer_invitation_date is not null )
	and classified."Customer Email" not like '%ollex%'