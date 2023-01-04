with customers as (
	                  select distinct on (id_customer) model.*
	                                                                                           , gedat."gedat_id"
	                                                                                           , kunden_cluster.cluster_id
	                                                                                           , kunden_cluster."ID_Level1"
	                                                                                           , kunden_cluster."ID_Level2"
	                                                                                           , kunden_cluster."Cluster"

	                  from {{ref('customer_table_horeca_children_customers_classified')}} as model
 								inner join sheet_loader.gedat_results as gedat
		                                 on model."Street" = gedat."gedat_strasse_1" or
		                                    model."Customer Name" = gedat."gedat_name_1"
		                       inner join sheet_loader."gedat" g on g.key::text = gedat."gedat_gt"::text
		                       inner join sheet_loader."kundencluster" as kunden_cluster
		                                 on kunden_cluster."cluster_id" = g.mapped_kx_id

                  )

select count( distinct "order".id_order )
	 , "Cluster"
	 , max( concat( extract( 'week' from "order".created_at ), '-',
	                extract( 'year' from "order".created_at ) ) ) "calender week"
	 , max( extract( 'week' from "order".created_at ) )           "week"
	 , max( extract( 'year' from "order".created_at ) )           "year"

from customers
	left join fdw_shop_service."order" on customers."UUID"::text  ="order".customer_uuid::text
	left join fdw_customer_service.merchant on "order".merchant_uuid::text = merchant.uuid::text
	left join sheet_loader.holdings on merchant.key  = holdings.merchant_key
where holding = 'gms'
  and cluster_id is not null
  and "order".created_at > now( ) - '64 day' ::interval

group by "Cluster", concat( extract( 'week' from "order".created_at ), '-',
                            extract( 'year' from "order".created_at ) )
order by year, week asc

