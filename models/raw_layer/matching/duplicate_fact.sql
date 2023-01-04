with
{% for GFGH in merchants_all() %}

tasch_{{GFGH}} as (select gfgh_product_id, p.name, sku, updated_at from {{ ref('gfgh_data') }} p
where merchant_key = '{{GFGH}}' and qa = TRUE)

,dups_{{GFGH}} as (select gfgh_product_id from tasch_{{GFGH}} group by 1 having count(*) > 1 )


{% if not loop.last %} 
, {% endif %}


{% endfor %}


{% for GFGH in merchants_all() %}

select '{{GFGH}}' as merchant_key, * from tasch_{{GFGH}} where gfgh_product_id in (select * from dups_{{GFGH}} )
{% if not loop.last %} 
union 
{% endif %}
{% endfor %}
