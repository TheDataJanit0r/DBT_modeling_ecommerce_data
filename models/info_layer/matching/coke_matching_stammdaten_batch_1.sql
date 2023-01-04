{{
    config(
        materialized='incremental',
      
    )
}}
with CTH_addresses_cleaned as (
	                              select *
		                               , replace( replace( concat( "Street", "Zip Code", "City" ), ' ', '' ), '.',
		                                          '' ) full_address
	                              from {{ref('customer_table_horeca_children_customers')}}
								   {% if is_incremental() %}

									-- this filter will only be applied on an incremental run
										where "Registration Date" >= CURRENT_DATE

									{% endif %}
                              )

   , coke_stammdaten as (

	                        select "Objektnummer"                                                          as stammnummer
			                     , "Objektname"                                                            as name
							, 'No Email'                                                                   as email
			                     , lower( concat( "Straße", ' ', "Postleitzahl", ' ', "Ort" ) ) as address
			                     , replace(
			                        replace( lower( concat( "Straße", ' ', "Postleitzahl", ' ', "Ort" ) ), '.', '' ), ' ',
			                        '' )                                                                   as full_address
	                        from stammdaten_jvp.coke_stammdaten
                            limit 50000

                        )
    , QR_form as (select "Wie lautet der vollständige Name deines Betriebs?" as coke_name
                      , "Wie lautet deine E-Mail-Adresse?"                  as coke_mail

                      , "Wie lautet deine Coca-Cola Stammnummer?"           as coke_stammnummer


                      , teamname

                      , "Submitted At"                                      as timestamp
                      , 'QR Form Express'                                   as coke_app
                      , "Token"
                 from sheet_loader.coke_express_type_form
                 union all
                 select "Wie lautet der vollständige Name deines Betriebs?" as coke_name
                      , "Wie lautet deine E-Mail-Adresse?"                  as coke_mail

                      , "Wie lautet deine Coca-Cola Stammnummer?"           as coke_stammnummer


                      , teamname

                      , "Submitted At"                                      as timestamp
                      , 'QR Form Shop'                                      as coke_app
                      , "Token"
                 from sheet_loader.coke_shop_type_form)
   , consolidated_coke_list_to_match as (select stammnummer
                                              , name
                                              , email
                                              , address
                                              , full_address
                                              , 'Keine Teamname im Stammdaten' as teamname
                                              , 'Stammdaten'                   as source
                                              , 'Keine Zeitstempel'            as timestamp
                                              ,'Kein Token'                    as Token
                                         from coke_stammdaten
                                         union all
                                         select coke_stammnummer
                                              , coke_name
                                              , coke_mail
                                              , 'Keine Address vorhanden vom QR Form' as address
                                              , 'Keine Address vorhanden vom QR Form' as full_address
                                              , teamname

                                              , coke_app                              as source
                                              , timestamp
                                              ,"Token"
                                         from QR_form)

   , matching as (select kxe.stammnummer::text                             "stammnummer"
                       , name                                              "Coke Name"
                       , kxe.email                                         "Coke Email"
                       , "Customer Name"
                       , cth."Customer Status"
                       , address
                       , cth.id_customer
                       , cth."Last Order Created"
                       , cth."Number Of Orders"
                       , cth."Registration Date"
                       , cth."Status Last Order"
                       , cth."Status Last Supplier Request"

                       , similarity(kxe.email, cth."Customer Email")    as distance_email
                       , similarity(kxe.name, cth."Customer Name")      as distance_name
                       , similarity(kxe.full_address, cth.full_address) as distance_address
                       , cth."Customer Email"                           as "Customer table horeca email"
                       , kxe.email                                      as "gsheet email"
                       , 'Keine Personal Nummer'                        as "Personalnummer Coke Mitarbeiter"
                       , kxe.source                                     as "Origin"
                       , 'No Team Name'                                 as "team_name"
                       , false                                          as "flag"
                       , "timestamp"
                       ,Token
                       ,kxe.full_address
                  from consolidated_coke_list_to_match kxe
                           cross join CTH_addresses_cleaned cth)
   , final as (select distinct on (id_customer) *
               from matching
               where (distance_email >= 0.8 or (distance_address >= 0.5 and distance_name >= 0.5)))


select  stammnummer                    as "Gsheet/Stammdaten Stammnummer"
     , "Coke Name"                    as "Gsheet/Stammdaten Name"
     , "Coke Email"                   as "Gsheet/Stammdaten Email"
     , id_customer                    as "Kollex Customer ID"
     , "Customer Name"                as "Kollex Horeca Name"
     , "Customer Status"              as "Kollex Status"
     , address                        as "Kollex Address"
     , 'KIP'                          as "Kollex App"
     , "Last Order Created"           as "Kollex Last Order"
     , "Status Last Order"            as "Kollex Last Order Status"
     , "Number Of Orders"                "Kollex Number Of Orders"
     , "Registration Date"            as "Kollex Registration Date"
     , "Status Last Supplier Request" as "Kollex Supplier Request"
     , case
           when "Customer Status" = 'Merchant Aktivierung austehend'
               then true
           else false
    end                               as "Kollex Activation by GFGH required"
     , "Customer table horeca email"     "Kollex Email"
     , "gsheet email"                 as "Gsheet Email"
     , "Personalnummer Coke Mitarbeiter"
     , "team_name"                    as "Coke Team Name"
     , "Origin"                       as "Ursprung"
     , case
           when flag is true
               then true
           when flag is null
               then false
           when flag is false
               then false
    end                               as "Gewinnspiel"
     , timestamp
    ,Token
    ,full_address
    ,distance_email
    ,distance_name
    ,distance_address
from final