select 
p.*
from {{ref('gfgh_data')}} p
 where 
 sku is null and 
 direct_shop_release = FALSE 
 and active = TRUE and kollex_active = TRUE and merchant_key not in {{ merchants_to_exclude() }}
 and special_case = FALSE

