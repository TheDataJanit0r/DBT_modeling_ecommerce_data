with main as (
   {% for GFGH in  merchants_active() %}

select
   '{{GFGH}}' AS gfgh,
   t.id AS id,
   t.status AS status,
   p.identifier AS sku 
from {{ ref(GFGH) }} t
join {{ ref('pim_catalog_product')}} p ON p.raw_values::JSON -> 'gfgh_{{GFGH}}_id' -> '<all_channels>' ->> '<all_locales>' = t.id 
where 
t.status <> 'enabled' 
and p.raw_values::JSON -> 'gfgh_{{GFGH}}_enabled' -> '<all_channels>' ->> '<all_locales>' = 'true' 

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}

)

,dups as (select distinct merchant_key gfgh, gfgh_product_id id from {{ref('duplicate_fact')}})

select * from main
left join dups using (gfgh,id)
where dups.id is null
