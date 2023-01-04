select
   p.identifier AS identifier,
   t.label AS category 
from
   (
(( {{ ref('pim_catalog_product') }} p 
      left join
         {{ref('pim_catalog_category_product')}} cp 
         on((cp.product_id = p.id))) 
      left join
         {{ref('pim_catalog_category')}} c 
         on((c.id = cp.category_id))) 
      left join
         {{ref('pim_catalog_category_translation')}} t 
         on(((t.foreign_key = c.id) 
         and 
         (
            t.locale = 'de_DE'
         )
))
   )
where
   t.label is not null
union all
select
   pm.code AS identifier,
   t.label AS label 
from
   (
(({{ref('pim_catalog_product_model')}} pm 
      left join
         {{ref('pim_catalog_category_product_model')}} cp 
         on((cp.product_model_id = pm.id))) 
      left join
         {{ref('pim_catalog_category')}} c 
         on((c.id = cp.category_id))) 
      left join
         {{ref('pim_catalog_category_translation')}} t 
         on(((t.foreign_key = c.id) 
         and 
         (
            t.locale = 'de_DE'
         )
))
   )
where
   t.label is not null
