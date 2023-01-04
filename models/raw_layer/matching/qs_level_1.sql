select distinct
   pm.raw_values::JSON->'golden_record_level1'->'<all_channels>'->>'<all_locales>' AS golden_record_level_1,
   pm.code AS level_1_code,
   v1.value AS type_single_unit,
   pm.raw_values::JSON->'detail_type_single_unit'->'<all_channels>'->>'<all_locales>' AS detail_type_single_unit,
   v2.value AS net_content_liter,
   g.data::JSON->'0 - Identifizierung   Klassifiz'->'NetContent[0]'->>'value' AS gs1_net_content,
   g.data::JSON->'0 - Identifizierung   Klassifiz'->'NetContent[0]'->>'unitOfMeasure' AS gs1_net_content_uom,
   g.data::JSON->'5 - Verpackung   Logistik   Pre'->>'WSODpgItem' AS gs1_dpg,
   pm.raw_values::JSON->'gtin_single_unit'->'<all_channels>'->>'<all_locales>' AS gtin_single_unit,
   pm2.raw_values::JSON->'title'->'<all_channels>'->>'<all_locales>' AS base_title,
   g.description AS gs1_desc,
   g.short_description AS gs1_short_desc,
   (
   (pm2.raw_values::JSON->'manufacturer_gln'->'<all_channels>'->>'<all_locales>')::text = (g.data::JSON->'0 - Identifizierung   Klassifiz'->'ManufacturerOfTradeItem[0]'->>'manufacturer')::text
   )
   AS equal_manufacturer_glns,
   pm2.raw_values::JSON->'manufacturer_gln'->'<all_channels>'->>'<all_locales>' AS mgln1,
   g.data::JSON->'0 - Identifizierung   Klassifiz'->'ManufacturerOfTradeItem[0]'->>'manufacturer' AS mgln2,

   {% for GFGH in  merchants_all()  %}
   (p.raw_values::JSON -> 'gfgh_{{GFGH}}_enabled' -> '<all_channels>' ->> '<all_locales>')::text as {{GFGH}},
   {% endfor %}
   
   (g.data::JSON->'0 - Identifizierung   Klassifiz'->'IsTradeItemABaseUnit')::text AS gs1_base_unit

from
   (
(((((( {{ref('pim_catalog_product_model')}} pm 
      left join
         {{ref('pim_catalog_product_model')}} pm2 
         on((pm2.id = pm.parent_id))) 
      left join
          {{ ref('pim_catalog_product') }} p 
         on((p.product_model_id = pm.id))) 
      left join
         {{ref('pim_catalog_attribute_option')}} o1 
         on((o1.code = pm.raw_values::JSON->'type_single_unit'->'<all_channels>'->>'<all_locales>'))) 
      left join
         {{ref('pim_catalog_attribute_option_value')}} v1 
         on((v1.option_id = o1.id))) 
      left join
         {{ref('pim_catalog_attribute_option')}} o2 
         on((o2.code = pm.raw_values::JSON->'net_content_liter'->'<all_channels>'->>'<all_locales>'))) 
      left join
         {{ref('pim_catalog_attribute_option_value')}} v2 
         on((v2.option_id = o2.id))) 
      left join
         {{ ref('gs1data') }} g 
         on((g.gtin_packaging_unit = pm.raw_values::JSON->'gtin_single_unit'->'<all_channels>'->>'<all_locales>'))
   )
where
   (
(pm.raw_values::JSON->'type_single_unit'->'<all_channels>'->>'<all_locales>' is not null) 
      and 
      (
         pm.raw_values::JSON->'golden_record_level1'->'<all_channels>'->>'<all_locales>' is null
         or 
         pm.raw_values::JSON->'golden_record_level1'->'<all_channels>'->>'<all_locales>' = 'false'
         
      )
      and 
      (
         pm2.raw_values::JSON->'status_base'->'<all_channels>'->>'<all_locales>' = '6a9b3601' 
         or pm.parent_id is null
      )
   )
