{{
config({
    "post-hook": [
      "{{ index(this, 'attribute_code')}}",
      "{{ index(this, 'option_code')}}"
    ],
    })
}}

select
   a.code AS attribute_code,
   o.code AS option_code,
   v.value AS value,
   now() as updated_at_utc 
from
   (
({{ref('pim_catalog_attribute_option_value')}} v 
      join
         {{ref('pim_catalog_attribute_option')}} o 
         on((o.id = v.option_id))) 
      join
         {{ref('pim_catalog_attribute')}} a 
         on((a.id = o.attribute_id))
   )
where
   (
      v.locale_code = 'de_DE'
   )
