with matching_ccep_to_merchants as (
	                                   select similarity( merchant.name::text , "CCEP_GFGH_Stammdaten"."Name"::text ) distance_name, *


	                                   from fdw_customer_service.merchant
		                                        cross join sheet_loader."CCEP_GFGH_Stammdaten"

                                   )

select distinct on ("CCEP Nr.")*
-- into prod_info_layer.ccep_stammdaten_merchants
from matching_ccep_to_merchants where distance_name>=0.8
