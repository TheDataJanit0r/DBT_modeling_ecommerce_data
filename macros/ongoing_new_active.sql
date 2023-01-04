{% macro ongoing_new_active(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   id,
   manufacturer,
   sku 
from
   {{ref(GFGH)}}
where
   sku is null and coalesce(direct_shop_release, 'false') <> 'true' and kollex_active = true
{% if not loop.last %} 
union 
{% endif %}

{% endfor %}


{% endmacro %}

