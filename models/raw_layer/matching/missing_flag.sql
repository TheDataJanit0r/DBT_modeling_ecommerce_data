select
   p.identifier AS identifier,
   'shop_enabled' AS shop_enabled 
from
   prod_raw_layer.all_skus p 
where
    shop_enabled is null
union

{% for GFGH in merchants_all()  %}


select
   p.identifier AS identifier,
   '{{GFGH}}_id' AS freigabe_{{GFGH}}_id 
from
 prod_raw_layer.all_skus p 
where
   {{GFGH}}_enabled is null
   

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}
