
{{ config(
    enabled=true,
    tags="updating gfgh_diff_weekly" ,
   
    post_hook= 
  [ 
     "update {{ this }} set name_diff = false where replace(name_old,' ','') = replace(name_new, ' ', '')"
  ,  "update {{ this }} set gtin_diff = false where gtin_old is null and gtin_new is not null or gtin_old is not null and gtin_new is null or gtin_new is null and gtin_old is null or gtin_new = gtin_old"
  
  ]
  
) }}
with skus as (
	             select merchant_key                                                             as gfgh_id
		              , gfgh_product_id
		              , id                                                                       as id_on_gfgh_data
		              , case when sku <> '' then coalesce( sku, direct_sku ) else direct_sku end as sku
	             from "dwh"."prod_raw_layer"."gfgh_data"
	             where merchant_key not in (
		                                       select *
		                                       from "dwh"."prod_raw_layer"."merchants_to_exclude"
	                                       )
--and ((sku is not null and qa = 'true') or direct_shop_release = TRUE)
             )

   , versions as (
	                 select *
			              , trim( (regexp_replace( replace( trim( pi0.name ), '"', '' ), ' +', ' ' )) ) regex_name
			              , pi0.created_at as                                                       imp_created_at
	                 from from_pim.cp_gfgh_product_import pi0
		                      join from_pim.cp_gfgh_import i on pi0.import_id::text = i.id::text
	                 where i.created_at::date not in ('2021-04-29', '2021-04-28', '2021-04-27')
			           and i.created_at::date >= CURRENT_DATE - 60

                 )


   , final as (
	              select distinct gfgh_id
	                            , gfgh_product_id
	                            , sku
	                            , id_on_gfgh_data
	                            , imp_created_at
	                            , to_char( imp_created_at, 'IYYY-IW' )                        as cal_week
	                            , regex_name                                                  as name
	                            , gtin
	                            , sales_unit_pkgg
	                            , no_of_base_units
	                            , case when manufacturer = '' then NULL else manufacturer end as manufacturer
	                            , base_unit_content
	                            , case
		                              when base_unit_content_uom = ''
			                              then NULL
		                              else base_unit_content_uom
	                              end                                                         as base_unit_content_uom
	              from versions
		                   join skus using (gfgh_id, gfgh_product_id)
	              order by 1, 2, 3
              )

   , ro as (
	           select *, row_number( )
	                     over (partition by gfgh_id, gfgh_product_id, cal_week order by imp_created_at desc) from final
           )

   , lags as (
	             select *
			          , lag( name ) over (partition by gfgh_id, gfgh_product_id order by cal_week) as name_old
			          , lag( gtin ) over (partition by gfgh_id, gfgh_product_id order by cal_week) as gtin_old

			          , lag( sales_unit_pkgg )
			            over (partition by gfgh_id, gfgh_product_id order by cal_week) as             sales_unit_pkgg_old
			          , lag( no_of_base_units )
			            over (partition by gfgh_id, gfgh_product_id order by cal_week) as             no_of_base_units_old

			          , lag( manufacturer )
			            over (partition by gfgh_id, gfgh_product_id order by cal_week) as             manufacturer_old
			          , lag( base_unit_content )
			            over (partition by gfgh_id, gfgh_product_id order by cal_week) as             base_unit_content_old

			          , lag( base_unit_content_uom )
			            over (partition by gfgh_id, gfgh_product_id order by cal_week) as             base_unit_content_uom_old

	             from ro
	             where row_number = 1
             )

   , real_final as (
	                   select gfgh_id
			                , gfgh_product_id
			                , sku
			                , id_on_gfgh_data
			                , imp_created_at
			                , cal_week
			                , name                                                             as name_new
			                , name_old
			                , name is distinct from name_old                                   as name_diff
			                , gtin                                                             as gtin_new
			                , gtin_old
			                , gtin is distinct from gtin_old                                   as gtin_diff
			                , sales_unit_pkgg                                                  as sales_unit_pkgg_new
			                , sales_unit_pkgg_old
			                , sales_unit_pkgg is distinct from sales_unit_pkgg_old             as sales_unit_pkgg_diff
			                , no_of_base_units                                                 as no_of_base_units_new
			                , no_of_base_units_old
			                , no_of_base_units is distinct from no_of_base_units_old           as no_of_base_units_diff
			                , manufacturer                                                     as manufacturer_new
			                , manufacturer_old
			                , manufacturer is distinct from manufacturer_old                   as manufacturer_diff
			                , base_unit_content                                                as base_unit_content_new
			                , base_unit_content_old
			                , base_unit_content is distinct from base_unit_content_old         as base_unit_content_diff
			                , base_unit_content_uom                                            as base_unit_content_uom_new
			                , base_unit_content_uom_old
			                , base_unit_content_uom is distinct from base_unit_content_uom_old as base_unit_content_uom_diff


	                   from lags
                   )

select *, concat( cal_week, id_on_gfgh_data ) cal_week_uuid
from real_final
where (name_diff = TRUE or gtin_diff = TRUE or sales_unit_pkgg_diff = TRUE or no_of_base_units_diff = TRUE
	or manufacturer_diff = TRUE or base_unit_content_diff = TRUE or base_unit_content_uom_diff = TRUE)

  and (name_old is not null
	or gtin_old is not null or sales_unit_pkgg_old is not null
	or no_of_base_units_old is not null
	or manufacturer_old is not null
	or base_unit_content_old is not null
	or base_unit_content_uom_old is not null)


  and id_on_gfgh_data <> ''
  and id_on_gfgh_data is not null