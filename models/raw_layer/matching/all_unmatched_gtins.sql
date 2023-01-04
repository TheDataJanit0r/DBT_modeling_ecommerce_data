select
   g.gtin_packaging_unit AS gtin_packaging_unit 
from {{ ref('all_gtins') }} g 
left join {{ ref('gs1data') }} gs1 on gs1.gtin_packaging_unit = g.gtin_packaging_unit 
where
   coalesce(gs1.gtin_packaging_unit) is null 
   and g.gtin_packaging_unit is not null

