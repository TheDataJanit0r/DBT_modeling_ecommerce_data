with gf as (select * from {{ref('gfgh_data')}})
{% for GFGH in  merchants_all() %}
    select '{{ GFGH }}'                                                       as gfgh
	 , '2'                                                                as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf

where merchant_key = '{{ GFGH }}'
  and active = 'true'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , '3'                                                                as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'false'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , '5'                                                                as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'true'
  and kollex_active = 'false'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , '8'                                                                as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'true'
  and kollex_active = 'true'
  and direct_shop_release = 'true'
  and ( category_code is not null or predicted_category is not null )
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , '9'                                                                as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'true'
  and kollex_active = 'true'
  and direct_shop_release = 'true'
  and category_code is null
  and predicted_category is null
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , '11'                                                               as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'true'
  and kollex_active = 'true'
  and coalesce( direct_shop_release , 'false' ) <> 'true'
  and ( sku is null or sku = '' )
group by 1, 2, 3, 4
union


select '{{ GFGH }}'                                                       as gfgh
	 , '14'                                                               as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( sku )                                                       as value
from gf
where merchant_key = '{{ GFGH }}'
  and active = 'true'
  and kollex_active = 'true'
  and coalesce( direct_shop_release , 'false' ) <> 'true'
  and qa = 'true'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , 'gastro_family'                                                    as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( sku )                                                       as value
from gf g
	     join {{ref ('sku_category_fact')}} s using (sku)
where merchant_key = '{{ GFGH }}' and active = 'true' and pim_family = 'Sonstiges (Gastro-Artikel)'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , 'sonderfaelle'                                                     as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gfgh_product_id )                                           as value
from gf
where merchant_key = '{{ GFGH }}'
  and special_case = 'true'
group by 1, 2, 3, 4
union

select '{{ GFGH }}'                                                       as gfgh
	 , 'pictures'                                                         as focus
	 , special_case::text
	 , case when gtin is not null and gtin <> '' then TRUE else FALSE end as gtin_boolean
	 , count( gf.sku )                                                    as value
from gf
	     join {{ref ('sku_pictures')}} using (sku)

where merchant_key = '{{ GFGH }}'
group by 1, 2, 3, 4

union
select '{{ GFGH }}'           as gfgh
	 , '13c'                  as focus
	 , special_case::text
	 , case
		   when coalesce( gtin_single_unit , gtin_packaging_unit ) is not null
			   then TRUE
		   else FALSE
	   end                    as gtin_boolean
	 , count( {{ GFGH }}.sku) as value
from {{ref( GFGH )}}
where coalesce( direct_shop_release , 'false' ) <> 'true'
  and ( status_base = 'Freigegeben' or status_base is null )
  and ( release_l1 = 'true' or release_l1 is null )
  and {{GFGH}}_enabled = 'false'
  and shop_enabled = 'false'

group by 1, 2, 3, 4

union

select '{{ GFGH }}'   as gfgh
	 , '15'           as focus
	 , q.special_case::text
	 , case
		   when coalesce( gtin_single_unit , gtin_packaging_unit ) is not null
			   then TRUE
		   else FALSE
	   end            as gtin_boolean
	 , count( q.sku ) as value
from {{ref( GFGH )}} q
join gf g
on g.gfgh_product_id::text =q.id::text and merchant_key = '{{ GFGH }}'
where coalesce (q.direct_shop_release
	, 'false') <> 'true'
  and (q.status_base = 'Freigegeben'
   or q.status_base is null)
  and (q.release_l1 = 'true'
   or q.release_l1 is null)
  and {{GFGH}}_enabled = 'true'
  and qa = 'false'
group by 1, 2, 3, 4


 {% if not loop.last %} union {% endif %}   
 {% endfor %}
