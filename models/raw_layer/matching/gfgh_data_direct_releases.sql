select 
p.*
from {{ref('gfgh_data')}} p
 where  direct_shop_release = TRUE and active = TRUE and kollex_active = TRUE 
 and merchant_key not in {{ merchants_to_exclude() }}
