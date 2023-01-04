select
   s.identifier AS identifier,
   s.gtin_packaging_unit AS gtin_packaging_unit,
   s.gtin_single_unit AS gtin_single_unit,
   s.amount_single_unit AS amount_single_unit,
   s.pim_family AS family,
   s.type_single_unit AS type_single_unit,
   s.title AS title,
   s.brand as brand,
   CONCAT(s.brand, ' ', s.title) AS brand_and_title,
   s.net_content AS net_content,
   s.net_content_uom AS net_content_uom,

   {% for GFGH in  merchants_all() %}
   case when s.{{GFGH}}_id is null or s.{{GFGH}}_id = '' then FALSE else TRUE end as {{GFGH}},
   {% endfor %}
   
   s.status_base AS status_base,
   s.shop_enabled AS shop_enabled,
   coalesce(gs1p.description, gs1s.description) AS gs1_desc,
   coalesce(gs1p.short_description, gs1s.short_description) AS gs1_short_desc,
   coalesce(gs1p.amount, gs1s.amount) AS gs1_amount,
   coalesce(gs1p.amount::text, gs1s.amount_text) AS gs1_amount_text,
   coalesce(gs1s.data::JSON->'0 - Identifizierung   Klassifiz'->'IsTradeItemABaseUnit') as gs1_base_unit,
   coalesce(gs1s.data::JSON->'0 - Identifizierung   Klassifiz'->'NetContent[0]'->>'value') as gs1_net_content
from
   (
( prod_raw_layer.all_skus s 
      left join
         {{ ref('gs1data') }} gs1p 
         on((gs1p.gtin_packaging_unit = s.gtin_packaging_unit))) 
      left join
         {{ ref('gs1data') }} gs1s 
         on(((gs1s.gtin_single_unit = s.gtin_single_unit) 
         and 
         (
            gs1s.gtin_packaging_unit = gs1s.gtin_single_unit
         )
))
   )
where
   (
      s.l1_code is null
      and 
      (
         s.base_code is not null
      )
      and 
      (
         s.status_base = 'Freigegeben'
      )
      and 
      (
         s.shop_enabled = 'false'
      )
   )
