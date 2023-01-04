{% macro ongoing_active(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   id,
   manufacturer,
   sku 
from {{ref(GFGH)}} 
where
   status = 'enabled' and kollex_active = 'true'  

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}


{% endmacro %}

