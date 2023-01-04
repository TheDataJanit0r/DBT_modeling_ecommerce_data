{{
    config(
        materialized='incremental'
		
    )
}}
with CTH_addresses_cleaned as (
	                              select *
										, replace( replace( concat( "Street"
																	, "Zip Code"
																	, "City" 
																)
															, ' '
															, '' 
														)
													, '.'
													,'' ) full_address
	                              from prod_info_layer.customer_table_horeca_children_customers
								   {% if is_incremental() %}

									-- this filter will only be applied on an incremental run
										where "Registration Date" >= CURRENT_DATE

									{% endif %}
                              )

   , coke_stammdaten as (

	                        select "Objektnummer"                                                    as stammnummer
			                     , "Objektname"                                                      as name

			                     , lower( concat( "Straße", ' ', "Postleitzahl", ' ', "Ort", ' ' ) ) as address
			                     , replace(
			                        replace( lower( concat( "Straße", "Postleitzahl", "Ort" ) ), '.', '' ), ' ',
			                        '' )                                                             as full_address
	                        from stammdaten_jvp.rotkaepchen_stammdaten

                        )
   , QR_form as (
	                select ruu_shop_typeform."Horeca Name" as coke_name
			             , "Email"                         as coke_mail

			             , "Stammnummer RuU__de"           as coke_stammnummer


			             , teamname

			             , "Submitted At__st"              as "timestamp"
			             , 'QR Form Shop'                  as coke_app
	                from sheet_loader.ruu_shop_typeform

                )
   , consolidated_coke_list_to_match as (
	                                        select stammnummer
			                                     , name
			                                     , 'Keine Email Addresse im Stammdaten' email
			                                     , address
			                                     , full_address
			                                     , 'Keine Teamname im Stammdaten' as    teamname
			                                     , 'Stammdaten'                   as    source
												 ,'Keine Zeitstempel'  as "timestamp"
	                                        from coke_stammdaten
	                                        union all
	                                        select coke_stammnummer
			                                     , coke_name
			                                     , coke_mail
			                                     , 'Keine Address vorhanden vom QR Form' as address
			                                     , 'Keine Address vorhanden vom QR Form' as full_address
			                                     , teamname

			                                     , coke_app                              as source
												 ,"timestamp"  as "timestamp"
	                                        from QR_form
                                        )


   , matching as (
	                 select kxe.stammnummer::text                               "stammnummer"
			              , name                                                "Coke Name"
			              , kxe.email                                           "Coke Email"
			              , "Customer Name"
			              , cth."Customer Status"
			              , address
			              , id_customer
			              , cth."Last Order Created"
			              , cth."Number Of Orders"
			              , cth."Registration Date"
			              , cth."Status Last Order"
			              , cth."Status Last Supplier Request"

			              , similarity( kxe.email, cth."Customer Email" )    as distance_email
			              , similarity( kxe.name, cth."Customer Name" )      as distance_name
			              , similarity( kxe.full_address, cth.full_address ) as distance_address
			              , cth."Customer Email"                             as "Customer table horeca email"
			              , kxe.email                                        as "gsheet email"
			              , 'Keine Personal Nummer'                          as "Personalnummer Coke Mitarbeiter"
			              , kxe.source                                       as "Origin"
			              , 'No Team Name'                                   as "team_name"
			              , false                                            as "flag"
						  ,"timestamp"
	                 from consolidated_coke_list_to_match kxe
		                      cross join CTH_addresses_cleaned cth

                 )
   , final as (
	              select distinct on (matching.stammnummer,"Customer Name","gsheet email") *
	              from matching
	              where (distance_email >= 0.8 or (distance_address >= 0.5 and distance_name >= 0.5))
			  )

              


select stammnummer                    as "Gsheet/Stammdaten Stammnummer"
	 , "Coke Name"                    as "Gsheet/Stammdaten Name"
	 , "Coke Email"                   as "Gsheet/Stammdaten Email"
	 , "Customer Name"                as "Kollex Horeca Name"
	 , id_customer                    as "Kollex Customer ID"
	 , "Customer Status"              as "Kollex Status"
	 , address                        as "Kollex Address"
	 , "Last Order Created"           as "Kollex Last Order"
	 , 'KIP'                          as "Kollex App"
	 , "Status Last Order"            as "Kollex Last Order Status"
	 , "Number Of Orders"                "Kollex Number Of Orders"
	 , "Registration Date"            as "Kollex Registration Date"
	 , "Status Last Supplier Request" as "Kollex Supplier Request"
	 , case
		   when "Customer Status" = 'Merchant Aktivierung austehend'
			   then true
		   else false
	   end                            as "Kollex Activation by GFGH required"
	 , "Customer table horeca email"     "Kollex Email"
	 , "gsheet email"                 as "Gsheet Email"
	 , "Personalnummer Coke Mitarbeiter"
	 , "team_name"                    as "Team Name"
	 , "Origin"                       as "Ursprung"
	 ,"timestamp"



--into prod_info_layer.coke_matched_stammdaten
from final
