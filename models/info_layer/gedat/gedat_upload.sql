with merchants as (
	                  select '01'                                                  as "Satzart"
		                   , '00000001'                                            as "Empfänger"
		                   , '43999023'                                            as "Absender"
		                   , '01'                                                  as "Versionsnummer"
		                   , concat( 'M_' , id_merchant )                          as "Hersteller-Kunden-Nr."
		                   , NULL                                                  as "GEDAT-Adressnummer"
		                   , NULL                                                  as "GLN"
		                   , NULL                                                  as "Leerfeld1"
		                   , NULL                                                  as "Leerfeld2"
		                   , NULL                                                  as "Gemeindekennziffer"
		                   , 'Getränkefachgroßhandel'                              as "Geschäftstyp"
		                   , merchant.name                                         as "Name-1 (Bezeichnung)"
		                   , NULL                                                  as "Name-2 (Inhaber)"
		                   , merchant.name                                         as "Kurzbezeichnung"
		                   , concat( address.street , ' ' , address.house_number ) as "Straße u. Hausnummer"
		                   , zip                                                   as "Postleitzahl"
		                   , city                                                  as "Ort"
		                   , 'Deutschland'                                         as "Land"
		                   , contact.mobile_phone                                  as "Telefon-1"
		                   , NULL                                                  as "Telefon-2"
		                   , NULL                                                  as "Leerfeld2"
		                   , NULL                                                  as "Telefax"
		                   , NULL                                                  as "Leerfeld3"
		                   , to_char( now( )::date , 'YYYYMMDD' )                  as "Übertragungsdatum"
		                   , 'N'                                                   as "Status"
		                   , '10000004'                                            as "Übertragungsnummer"
	                  from fdw_customer_service.merchant
		                       left join fdw_customer_service.merchant_has_contacts
			                                 on merchant.id_merchant = merchant_has_contacts.fk_merchant
		                       left join fdw_customer_service.contact on merchant_has_contacts.fk_contact
		                  = contact.id_contact
		                       left join fdw_customer_service.address on address.id_address = merchant.fk_address
	                  where merchant.name not like '%%test%%'
		                and merchant.name not like '%%Test%%'
		                and merchant.name not like '%löschen%'
		                and merchant.name not like '%test%'
		                and merchant.name not like '%kollex%'
		                and merchant.name not like '%"%'
		                and merchant.name not like '-%'
		                and merchant.name is not null

		                and merchant.name not like '%inactive%'

		                and merchant.name not like '%*%'
		                and merchant.name not like '%#%'
		                and merchant.name not like '%AAA%'
		                and merchant.fk_address is not null
		                and address.street is not null
		                and address.street <> ''
		                and address.street not like '%-1%'
		                and address.street not like ' '
		                and address.street like '%.%'
		                and address.street not like '-%'
		                and address.street not like ',%'
		                and address.street not like '%,-%'
		                and address.city not like '%not provided%'
		                and lower( address.street ) not like '%c/o%'
		                and lower( address.zip ) not like ' '
		                and lower( address.zip ) not like '%d%'
		                and lower( address.zip ) not like '%e%'
		                and length( address.zip ) >= 5
		                and address.zip not like '%n.a%'
		                and address.zip not like '%not provided%'
		                and merchant.name not like ',%'
		                and merchant.name not like '-%'
		                and merchant.name not like ':%'
		                and merchant.name not like '%Kuba%'
		                and merchant.name not like '?%'
		                and merchant.name not like '„%'
		                and merchant.name not like '.%'
		                and merchant.name not like '(%'

		                and merchant.name <> '1:0'

	                  --and length( address.city ) >= 5
                  )


   , customers as (
	                  select '01'                                 as "Satzart"
			               , '00000001'                           as "Empfänger"
			               , '43999023'                           as "Absender"
			               , '01'                                 as "Versionsnummer"
			               , concat( 'O_' , id_customer )         as "Hersteller-Kunden-Nr."
			               , NULL                                 as "GEDAT-Adressnummer"
			               , NULL                                 as "GLN"
			               , NULL                                 as "Leerfeld"
			               , NULL                                 as "Leerfeld1"
			               , NULL                                 as "Gemeindekennziffer"
			               , NULL                                 as "Geschäftstyp"
			               , cth."Customer Name"                  as "Name-1 (Bezeichnung)"
			               , cth."Customer Name"                  as "Name-2 (Inhaber)"
			               , cth."Customer Name"                  as "Kurzbezeichnung"
			               , concat( cth."Street" )               as "Straße u. Hausnummer"
			               , cth."Zip Code"                       as "Postleitzahl"
			               , cth."City"                           as "Ort"
			               , 'Deutschland'                        as "Land"
			               , cth.mobile_phone                     as "Telefon-1"
			               , NULL                                 as "Telefon-2"
			               , NULL                                 as "Leerfeld2"
			               , NULL                                 as "Telefax"
			               , NULL                                 as "Leerfeld3"
			               , to_char( now( )::date , 'YYYYMMDD' ) as "Übertragungsdatum"
			               , 'N'                                  as "Status"
			               , '10000004'                           as "Übertragungsnummer"
	                  from prod_info_layer.customer_table_horeca_children_customers as cth
	                  where "Customer Name" not like '%löschen%'
			            and "Customer Name" not like '%test%'
			            and "Customer Name" not like '%"%'
			            and "Customer Name" not like '%inactive%'

			            and "Customer Name" not like '%*%'
			            and "Customer Name" not like '%#%'

			            and "Customer Name" not like '%kollex%'
			            and "Customer Name" is not null
			            and "Customer Email" not like '%test%'
			            and "Customer Email" not like '%kollex%'
			            and "Customer Email" not like '%löschen%'
			            and "Customer Name" not like '%AAA%'
			            and "Street" is not null
			            and "Street" not like '%- -%'
			            and "Street" <> ''
			            and "Street" not like ' '
			            and "Street" not like '%-%'
			            and "Street" not like '%,-%'
			            and "Street" not like '%kollex%'
			            and "City" not like '%kollex%'
			            and "Street" not like 'Römermauer 3'
			            and "Street" not like 'Stralauer Allee 4'
			            and "Street" not like 'Sektkellereistraße 5'
			            and "Street" not like 'Konrad-Zuse-Weg 1'
			            and "Street" not like 'Karl-Herdt-Weg 100'
			            and "Street" not like 'Hagener Straße 261'
			            and "Street" not like '%not provided%'
			            and "Street" not like 'Elbestraße 1-3'
			            and "Street" not like 'Elbestraße 1-3'
			            and "Street" not like '-%'
			            and "Street" not like ',%'
			            and "Customer Name" not like ',%'
			            and "Customer Name" not like '-%'
			            and "Customer Name" not like ':%'
			            and "Customer Name" not like '%Kuba%'
			            and "Customer Name" not like '?%'
			            and "Customer Name" not like '„%'
			            and "Customer Name" not like '.%'
			            and "Customer Name" not like '(%'
			            and "Customer Name" <> '1:0'
			            and "Street" not like '%Test%'
			            and "Street" not like 'Zenettistraße 11'
-- 			            and "Street" like '%.%'
			            and "City" not like '%not provided%'
			            and lower( "Street" ) not like '%c/o%'
			            and lower( "Zip Code" ) not like ' '
			            and lower( "Zip Code" ) not like '%d%'
			            and lower( "Zip Code" ) not like '%e%'
			            and length( "Zip Code" ) >= 5
			            and "Zip Code" not like '%n.a%'
			            and "Zip Code" not like '%not provided%'
	                  --and length( "City" ) >= 5

                  )


   , final as (
	              select *
	              from customers

	              union
	              select *
	              from merchants
              )

select *
from final
where "Straße u. Hausnummer" <> ''