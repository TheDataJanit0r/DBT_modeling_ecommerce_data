with all_customers as (
	                  select distinct on (id_customer) model.*
	                                                 , gedat."gedat_id"
	                                                 , kunden_cluster.cluster_id
	                                                 , kunden_cluster."ID_Level1"
	                                                 , kunden_cluster."ID_Level2"
	                                                 , holding


	                  from {{ref('customer_table_horeca_children_customers_classified')}}  as model
		                       left join sheet_loader.gedat_results as gedat
		                                 on model."Street" = gedat."gedat_strasse_1" or
		                                    model."Customer Name" = gedat."gedat_name_1"
		                       left join sheet_loader."gedat" g on g.key::text = gedat."gedat_gt"::text
		                       left join sheet_loader."kundencluster" as kunden_cluster
		                                 on kunden_cluster."cluster_id" = g.mapped_kx_id
		                       left join fdw_customer_service.customer_has_merchant
		                                 on model.id_customer = fk_customer
		                       left join fdw_customer_service.merchant on fk_merchant = id_merchant
		                       left join prod_raw_layer.holdings on merchant_key = merchant.key
                    where customer_has_merchant.status like '%REGISTERED%'


                  )
   , all_gms as (
	                select count( distinct id_customer )::numeric as "count"
	                from all_customers
	                where holding = 'tkk'
                )
select count / (
	                                       select count(distinct id_customer)
	                                       from all_customers
                                       )
from all_gms




