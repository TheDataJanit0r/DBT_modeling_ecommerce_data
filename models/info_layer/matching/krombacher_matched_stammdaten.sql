{{
    config(
        materialized='incremental',
        unique_key='id_customer'
    )
}}
with krombacher_customers_addresses_cleaned as
	(
		select *
			 , replace( replace( lower( concat( "Straße", "Postleitzahl", "Ort" ) ), '.', '' ), ' ',
			            '' ) full_address
		from stammdaten_jvp.krombacher_stammdaten
	)
   , CTH_addresses_cleaned as (
	                              select *
			                           , replace( replace( concat( "Street", "Zip Code", "City" ), ' ', '' ), '.',
			                                      '' ) full_address
	                              from prod_info_layer.customer_table_horeca_children_customers
								   {% if is_incremental() %}

									-- this filter will only be applied on an incremental run
										where "Registration Date" >= CURRENT_DATE

									{% endif %}
                              )

   , matching as (
	                 select kxe."Objektnummer"
			              , "Objektname"
			              , "Straße"
			              , "Postleitzahl"
			              , "Ort"
			              , cth.*
			              , similarity( kxe.full_address, cth.full_address )  as distance
			              , similarity( kxe."Objektname", cth."Customer Name" ) as distance_2
			              , cth."Customer Email"                              as "Customer table horeca email"


	                 from krombacher_customers_addresses_cleaned as kxe
		                      cross join CTH_addresses_cleaned cth

                 )


select distinct on (id_customer) *
,'Keine Zeitstempel' as "timestamp"
--into prod_info_layer.krombacher_matched_stammdaten
from matching



where distance >= 0.5
   and distance_2 >= 0.5