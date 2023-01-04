

with kollex_user_extract_origin as (
	                                   select *
		                                    , (additional_data -> 'sources' -> 0)::text as origin_1
		                                    , (additional_data -> 'sources' -> 1)::text as origin_2
	                                   from  prod_info_layer.customer_table_horeca_children_customers cth
		                                        left join fdw_customer_service.customer_has_user_and_role
		                                                  on customer_has_user_and_role.fk_customer =
		                                                     cth.id_customer
		                                        left join fdw_customer_service."user"
		                                                  on "user".id_user = customer_has_user_and_role.fk_user




                                   )
   , all_skus_manufacturer_cleaned as (
	                                      select all_skus.identifier

			                                   , all_skus.base_code
			                                   , all_skus.status_base

			                                   , concat( '|',
			                                             case
				                                             when all_skus.manufacturer_name = 'None'
					                                             then all_skus.manufacturer
				                                             else all_skus.manufacturer_name
			                                             end,
			                                             '|' ) manufacturer_name
	                                      from prod_raw_layer.all_skus

                                      )

   , KIP_order_items as (
	                        select distinct on (order_item.id_order_item
		                        ,kollex_user_extract_origin.uuid) order_item.id_order_item
	                                                            , order_item.amount
	                                                            , order_item.created_at
	                                                            , order_item.updated_at
	                                                            , order_item.order_item_uuid
	                                                            , order_item.fk_order
	                                                            , order_item.fk_product
-- 	                                                            , product_name
	                                                            , order_item.product_packaging_unit
	                                                            , order_item.product_package
	                                                            , order_item.order_item_meta
	                                                            , all_skus_manufacturer_cleaned.identifier

	                                                            , all_skus_manufacturer_cleaned.base_code
	                                                            , all_skus_manufacturer_cleaned.status_base

	                                                            , all_skus_manufacturer_cleaned.manufacturer_name
	                                                            , case
		                                                              when all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%ola%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%oca%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%polli%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%onster%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%odenthal%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%deZ%' or
			                                                               order_item.product_name::text like
			                                                               '%onaqa%' or
			                                                               order_item.product_name::text like
			                                                               '%CHAQWA%' or
			                                                               order_item.product_name::text like
			                                                               '%Costa%' or
			                                                               order_item.product_name::text like
			                                                               '%Fanta%'
		                                                                  or
		                                                                  order_item.product_name::text like
			                                                               '%ola%' or
			                                                               order_item.product_name::text like
			                                                               '%oca%' or
			                                                               order_item.product_name::text like
			                                                               '%polli%' or
			                                                               order_item.product_name::text like
			                                                               '%onster%' or
			                                                               order_item.product_name::text like
			                                                               '%odenthal%'
		                                                                  or
		                                                                  order_item.product_name::text like
			                                                               '%ruitopia%' or
			                                                               order_item.product_name::text like
			                                                               '%uze%' or
			                                                               order_item.product_name::text like
			                                                               '%lacéau%' or
			                                                               order_item.product_name::text like
			                                                               '%Honest%' or
			                                                               order_item.product_name::text like
			                                                               '%Kinley%'
		                                                                  or
		                                                                  order_item.product_name::text like
			                                                               '%Lift%' or
			                                                               order_item.product_name::text like
			                                                               '%mezzo%' or
			                                                               order_item.product_name::text like
			                                                               '%Maid%' or
			                                                               order_item.product_name::text like
			                                                               '%Powerade%' or
			                                                               order_item.product_name::text like
			                                                               '%Reign%'
		                                                                  or
		                                                                  order_item.product_name::text like
			                                                               '%Royal%' or
			                                                               order_item.product_name::text like
			                                                               '%Sprit%' or
			                                                               order_item.product_name::text like
			                                                               '%Chico%' or
			                                                               order_item.product_name::text like
			                                                               '%URBACHER%' or
			                                                               order_item.product_name::text like
			                                                               '%ViO%'
			                                                              then true
		                                                              else false
	                                                              end as                          "c"
	                                                            , case
		                                                              when all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%rombach%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%chwep%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%rombach%' or
			                                                               order_item.product_name::text like
			                                                               '%chwep%'
																			or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%urgsteinfurter%' or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%henania%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%urgsteinfurter%' or
			                                                               order_item.product_name::text like
			                                                               '%henania%'
																		or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%itamalz%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%itamalz%'
		                                                                  or
		                                                                   all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%olinck%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%olinck%'
			                                                              then true
		                                                              else false
	                                                              end as                          "k"
	                                                            , case
		                                                              when all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%itburg%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%raft%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%östrit%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%önig%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%erolstein%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%oblenz%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%itburg%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%raft%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%östrit%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%önig%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%erolstein%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%oblenz%'
			                                                              then true
		                                                              else false
	                                                              end as                          "b"
	                                                            , case
		                                                              when all_skus_manufacturer_cleaned.manufacturer_name like
		                                                                   '%Château %'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%et Danske%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%omaine%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%émy%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%etzer%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%ratelli%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%eartland%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%todden-%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%onstantia%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%aborie %'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%archesi%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%Masi%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%Moskovskaya%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%Codorníu%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%Reidemeister%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%otkäppch%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%ojeña%'
			                                                              or
			                                                               all_skus_manufacturer_cleaned.manufacturer_name like
			                                                               '%Weingut %'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Château %'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%et Danske%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%omaine%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%émy%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%etzer%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%ratelli%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%eartland%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%todden-%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%onstantia%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%aborie %'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%archesi%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Masi%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Moskovskaya%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Codorníu%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Reidemeister%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%otkäppch%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%ojeña%'
			                                                              or
			                                                               order_item.product_name::text like
			                                                               '%Weingut %'
			                                                              then true
		                                                              else false
	                                                              end as                          "r"

	                                                            , "order".user_uuid
	                                                            , "order".merchant_uuid
	                                                            , "order".status
	                                                            , "order".created_at
	                                                            , "order".updated_at
	                                                            , "order".id_order

	                                                            , "order".customer_uuid
	                                                            , kollex_user_extract_origin.email
	                                                            , kollex_user_extract_origin.uuid "user_uuid_express_items"
	                                                            , kollex_user_extract_origin.*
	                                                            , case
		                                                              when kollex_user_extract_origin.origin_1 like
		                                                                   '%ola%' or
		                                                                   kollex_user_extract_origin.origin_2 like
		                                                                   '%ola%'
			                                                              then true
	                                                              end                             "Cola Origin"
	                                                            , case
		                                                              when kollex_user_extract_origin.origin_1 like
		                                                                   '%burger%' or
		                                                                   kollex_user_extract_origin.origin_2 like
		                                                                   '%burger%'
			                                                              then true
	                                                              end                             "Bitburger Origin"
	                                                            , case
		                                                              when kollex_user_extract_origin.origin_1 like
		                                                                   '%bacher%' or
		                                                                   kollex_user_extract_origin.origin_2 like
		                                                                   '%bacher%'
			                                                              then true
	                                                              end                             "Krombacher Origin"
	                                                            , case
		                                                              when kollex_user_extract_origin.origin_1 like
		                                                                   '%pchen%' or
		                                                                   kollex_user_extract_origin.origin_1 like
		                                                                   '%pchen%'
			                                                              then true
	                                                              end                             "Rot Origin"


	                        from kollex_user_extract_origin
		                             left join fdw_shop_service."order"
		                                       on kollex_user_extract_origin."UUID"::text = "order".customer_uuid::text
		                             left join fdw_shop_service."order_item" "order_item"
		                                       on "order_item".fk_order = "order".id_order
		                             left join fdw_shop_service.product
		                                       on order_item.fk_product = product.id_product
		                             left join from_pim.cp_gfgh_product
		                                       on cp_gfgh_product.direct_sku::text = product.direct_sku::text
		                             left join all_skus_manufacturer_cleaned
		                                       on all_skus_manufacturer_cleaned.identifier::text = cp_gfgh_product.sku::text


                        )


   , grouping_KIP_items as (
	                           select bool_or( coalesce( r, "Rot Origin" ) )        "Rottkapechen Kunde"
			                        , bool_or( coalesce( k, "Krombacher Origin" ) ) "Krombacher Kunde"
			                        , bool_or( coalesce( b, "Bitburger Origin" ) )  "Bitburger Kunde"
			                        , bool_or( coalesce( c, "Cola Origin" ) )       "Coke Kunde"
			                        , id_customer
	                           from KIP_order_items
	                           group by id_customer
                           )

------   the reason here being that group_kip is people who have ordered something before and most of the OBIS numbers are actually merchant AKTIVIERUNG AUSTEHEND
------   so they won't appear in that join and they would be dropped from the list
------   i need to join the stammdaten on all customer list and not this group only so that i don't have gaps in the joins
   , joining_with_stammdaten as
(
	select "Rottkapechen Kunde"
		 , "Krombacher Kunde"
		 , "Bitburger Kunde"
		 , "Coke Kunde"
	     , bitburger_matched_stammdaten."Objektnummer"
		 , case
			   when bitburger_matched_stammdaten."Objektnummer" is not null
				   then true
			   else false
		   end bitburger_nummer
		 , case
			   when krombacher_matched_stammdaten."Objektnummer" is not null
				   then true
			   else false
		   end krombacher_nummer
		 , case
			   when coke_matched_stammdaten."Gsheet/Stammdaten Stammnummer" is not null
				   then true
			   else false
		   end coke_nummer
		 , case
			   when rotkaepchen_matched_stammdaten."Gsheet/Stammdaten Stammnummer" is not null
				   then true
			   else false
		   end rotkaepchen_nummer
	,customer_table_horeca_children_customers.*
	,customer_table_horeca_children_customers.id_customer CTH_ID_CUSTOMER


	from prod_info_layer.customer_table_horeca_children_customers

			 left join grouping_KIP_items  using (id_customer)
		     left join "dwh"."prod_info_layer"."krombacher_matched_stammdaten"
		               on customer_table_horeca_children_customers.id_customer =
		                  krombacher_matched_stammdaten.id_customer
		     left join "dwh"."prod_info_layer"."rotkaepchen_matched_stammdaten"
		               on customer_table_horeca_children_customers.id_customer =
		                  rotkaepchen_matched_stammdaten."Kollex Customer ID"
		     left join "dwh"."prod_info_layer"."coke_matched_stammdaten"
		               on customer_table_horeca_children_customers.id_customer =
		                  coke_matched_stammdaten."Kollex Customer ID"
		     left join "dwh"."prod_info_layer"."bitburger_matched_stammdaten"
		               on customer_table_horeca_children_customers.id_customer =
		                  bitburger_matched_stammdaten.id_customer

)


, final as (
	              select distinct on (CTH_ID_CUSTOMER) CTH_ID_CUSTOMER as id_customer
												, customer."Customer Name"
												, customer."Customer Email"
												, customer."Customer Status"
												, customer."Number Of Orders"
												, customer."Registration Date"
												, customer.customer_invitation_date
												, customer."Last Order Created"
												, customer."UUID"
												, customer."Status Last Order"
												, customer."Last Successful List Upload"
												, customer."Status Last Supplier Request"
												, customer."GFGH"
												, customer."Street"
												, customer."Zip Code"
												, customer."City"
												, customer."Customer_reactivation_flag"
												, customer."Last Order last 3 months"
												, customer."First Order last 3 months"
												, customer."average days between orders"
												, customer."average days between orders last 3 months"
												, customer."Number Of Orders last 3 months"
												, customer.mobile_phone

	                                             ,case when          "Rottkapechen Kunde" is true
	                                                              or  rotkaepchen_nummer is true
                                                                  or  rotkaepchen_matched_stammdaten."Gsheet/Stammdaten Stammnummer" is not null

			                                                         then true
		                                                         else false
                                                       end                                    as  "Rottkapechen Kunde"
												,case when           "Coke Kunde" is true
	                                                              or  coke_nummer is true
                                                                  or  coke_matched_stammdaten."Gsheet/Stammdaten Stammnummer" is not null

			                                                         then true
		                                                         else false
                                                     end  as  "Coke Kunde"
	                                             , case when          "Krombacher Kunde" is true
	                                                              or  krombacher_nummer is true

			                                                         then true
		                                                         else false
                                                         end   as "Krombacher Kunde"
	                                             , case when          "Bitburger Kunde" is true
	                                                              or  bitburger_nummer is true

			                                                         then true
		                                                         else false
	                                                         end    "Bitburger Kunde"

		                 from joining_with_stammdaten customer
		                   left join prod_info_layer.rotkaepchen_matched_stammdaten
		                             on customer.CTH_ID_CUSTOMER =
		                                rotkaepchen_matched_stammdaten."Kollex Customer ID"
		                   left join prod_info_layer.coke_matched_stammdaten
		                             on customer.CTH_ID_CUSTOMER = coke_matched_stammdaten."Kollex Customer ID"

              )


select *
from final



