{% macro ongoing_nonexisting(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   s.identifier as sku,
   k.manufacturer,
   s.{{GFGH}}_id as gfgh_id
from
  prod_raw_layer.all_skus s
left join 
   {{ref(GFGH)}} k on k.id = s.{{GFGH}}_id
where
s.{{GFGH}}_enabled = 'true' and k.id is null

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}


{% endmacro %}
