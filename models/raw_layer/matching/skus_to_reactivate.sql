with main as (
{% for GFGH in  merchants_active()  %}
select
   '{{GFGH}}' AS gfgh,
   k.id AS id,
   k.name AS name,
   k.status AS status,
   k.kollex_active AS kollex_active,
   s.{{GFGH}}_enabled AS gfgh_enabled,
   s.{{GFGH}}_id AS gfgh_id,
   s.identifier as sku 
from {{ ref(GFGH) }} k join prod_raw_layer.all_skus s on s.{{GFGH}}_id = k.id
where k.status = 'enabled' and (s.{{GFGH}}_enabled = 'False' or s.{{GFGH}}_id is null)

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}

)

,dups as (select distinct merchant_key gfgh, gfgh_product_id id from {{ref('duplicate_fact')}})

select * from main
left join dups using (gfgh,id)
where dups.id is null
