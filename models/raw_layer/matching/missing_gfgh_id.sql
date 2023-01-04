

{% for GFGH in merchants_all() %}


select
   '{{GFGH}}' AS gfgh,
   p.identifier AS sku 
from
  prod_raw_layer.all_skus p 
where
   (
      {{GFGH}}_enabled = 'true' and
      {{GFGH}}_id is null
   )
{% if not loop.last %} 
union 
{% endif %}

{% endfor %}

