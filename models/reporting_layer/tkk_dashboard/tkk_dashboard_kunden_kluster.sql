with customers as (
	                  select distinct on (id_customer) model.*
	                                                 , gedat."gedat_id"
	                                                 , kunden_cluster.cluster_id
	                                                 , kunden_cluster."ID_Level1"
	                                                 , kunden_cluster."ID_Level2"
	                                                 , kunden_cluster."Cluster"

	                  from {{ref('customer_table_horeca_children_customers_classified')}}  as model
		                       inner join sheet_loader.gedat_results as gedat
		                                 on model."Street" = gedat."gedat_strasse_1" or
		                                    model."Customer Name" = gedat."gedat_name_1"
		                       inner join sheet_loader."gedat" g on g.key::text = gedat."gedat_gt"::text
		                       inner join sheet_loader."kundencluster" as kunden_cluster
		                                 on kunden_cluster."cluster_id" = g.mapped_kx_id

                  )

select count( distinct id_customer ),
       case when "Cluster" is null then 'Rest' else "Cluster" end as "Cluster"
from customers
	     left join fdw_customer_service.customer_has_merchant
	               on customers.id_customer = customer_has_merchant.fk_customer
	     left join fdw_customer_service.merchant on customer_has_merchant.fk_merchant = merchant.id_merchant
	     left join prod_raw_layer.holdings on merchant.key = holdings.merchant_key
where holding = 'tkk'
group by "Cluster"



