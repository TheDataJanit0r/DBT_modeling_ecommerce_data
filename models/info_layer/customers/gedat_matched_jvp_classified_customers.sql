select distinct on (model.id_customer) model.*
                                     , gedat.gedat_id
                                     , kunden_cluster.cluster_id
                                     , kunden_cluster."ID_Level1"
                                     , kunden_cluster."ID_Level2"
                                     , kunden_cluster."Sub-Cluster"
from {{ref('customer_table_horeca_children_customers_classified')}} as model

	     left join sheet_loader.gedat_results as gedat
	               on model."Street" = gedat.gedat_strasse_1 or
	                  model."Customer Name" = gedat.gedat_name_1
	     left join sheet_loader.gedat g on g.key::text = gedat.gedat_gt::text
	     left join sheet_loader.kundencluster as kunden_cluster
	               on kunden_cluster."cluster_id" = g.mapped_kx_id
