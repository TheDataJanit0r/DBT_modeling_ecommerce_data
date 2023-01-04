select
   s.identifier AS sku,
   s.gtin_packaging_unit AS gtin_packaging_unit,
   gs1.description AS gs1_desc,
   gs1.short_description AS gs1_short_desc,
   s.title AS title1,
   CONCAT(s.brand, ' ', s.title) AS brand_and_title,
   s.type_single_unit AS type_single_unit,
   s.detail_type_single_unit AS detail_type_single_unit,
   s.amount_single_unit AS amount_single_unit,
   s.title AS title2,
   s.structure_packaging_unit AS structure_packaging_unit,
   s.type_packaging_unit AS type_packaging_unit,
   s.detail_type_packaging_unit AS detail_type_packaging_unit,
   s.brand AS brand,
   s.title AS title3,
   s.net_content AS net_content,
   gs1.data::JSON->'0 - Identifizierung   Klassifiz'->'NetContent[0]'->>'value' as gs1_net_content,
   gs1.data::JSON->'0 - Identifizierung   Klassifiz'->'NetContent[0]'->>'unitOfMeasure' as gs1_net_content_uom,
   gs1.data::JSON->'text_base'->'<all_channels>'->>'<all_locales>' as shop_enabled
from
   (
(prod_raw_layer.all_skus s 
      join
         {{ ref('pim_catalog_product') }} p 
         on((s.identifier = p.identifier))) 
      left join
         {{ ref('gs1data') }} gs1 
         on((gs1.gtin_packaging_unit = s.gtin_packaging_unit))
   )
where
   (
      TRUE 
      and 
      (
         s.shop_enabled = 'false'
      )
      and 
      (
         s.pim_family like 'Mix%'
      )
   )
