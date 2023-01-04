{% macro ongoing_unmatched(GFGH_LIST) %}

select
   T.gfgh AS gfgh,
   T.gtin_packaging_unit,
   T.id AS id,
   T.no_of_base_units,
   T.base_unit_content,
   T.direct_shop_release,
   T.packaging_unit AS packaging_unit,
   T.manufacturer,
   T.name AS name,
   T.sku AS sku,
   T.kollex_active AS kollex_active,
   T.tool_link
from
   (    

{% for GFGH in GFGH_LIST %}

select
         '{{GFGH}}' AS gfgh,
         t.gpu as gtin_packaging_unit,
         t.manufacturer,
         t.no_of_base_units,
         t.base_unit_content,
         t.direct_shop_release,
         t.id AS id,
         t.packaging_unit AS packaging_unit,
         t.name AS name,
         t.sku AS sku,
         t.kollex_active AS kollex_active,
         t.tool_link
      from
         
            {{ref(GFGH)}} t 
           inner join
            {{ ref('all_unmatched_gtins') }} u 
               on u.gtin_packaging_unit = t.gpu 
                  and coalesce(t.direct_shop_release, 'false') <> 'true'
               
         
         where t.kollex_active = true
{% if not loop.last %} 
union 
{% endif %}

{% endfor %}

   )   
   T 
where
   T.sku is null and T.kollex_active
order by
   T.gtin_packaging_unit





{% endmacro %}

