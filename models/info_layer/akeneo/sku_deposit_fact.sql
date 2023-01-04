WITH base AS (

SELECT

p.identifier::VARCHAR(20) as sku
,dep_single.code::VARCHAR(20)
,unit_val.value::INT AS units
,CASE
  WHEN dep_single_v.value IS NULL THEN 0.00
  WHEN dep_single_v.value LIKE '%,%' THEN REPLACE(REPLACE(dep_single_v.value, ' €', ''), ',', '.')::NUMERIC(10,2)
  WHEN dep_single_v.value = 'Kein Pfand' THEN NULL
  WHEN dep_single_v.value LIKE 'leer%' THEN NULL
  ELSE NULL
END AS single_deposit
,CASE
  WHEN dep_pack_v.value IS NULL THEN 0.00
  WHEN dep_pack_v.value LIKE '%,%' THEN REPLACE(REPLACE(dep_pack_v.value, ' €', ''), ',', '.')::NUMERIC(10,2)
  WHEN dep_pack_v.value = 'Kein Pfand' THEN NULL
  WHEN dep_pack_v.value LIKE 'leer%' THEN NULL
  ELSE NULL
END AS package_deposit


FROM {{ref('pim_catalog_product')}} p
LEFT JOIN {{ref('pim_catalog_product_model')}} pm
    ON p.product_model_id = pm.id

LEFT JOIN {{ref('pim_catalog_attribute_option')}} dep_single
    ON pm.raw_values::JSON->'deposit_value_single_unit'->'<all_channels>'->>'<all_locales>' = dep_single.code
    AND dep_single.attribute_id = 26
LEFT JOIN {{ref('pim_catalog_attribute_option_value')}} dep_single_v
    ON dep_single.id = dep_single_v.option_id

LEFT JOIN {{ref('pim_catalog_attribute_option')}} unit
    ON p.raw_values::JSON->'amount_single_unit'->'<all_channels>'->>'<all_locales>' = unit.code
    AND unit.attribute_id = 20
LEFT JOIN {{ref('pim_catalog_attribute_option_value')}} unit_val
    ON unit.id = unit_val.option_id

LEFT JOIN {{ref('pim_catalog_attribute_option')}} dep_pack
    ON p.raw_values::JSON->'deposit_value_packaging_unit'->'<all_channels>'->>'<all_locales>' = dep_pack.code
    AND dep_pack.attribute_id = 23
LEFT JOIN {{ref('pim_catalog_attribute_option_value')}} dep_pack_v
    ON dep_pack.id = dep_pack_v.option_id

WHERE unit_val.value IS NOT NULL

)

SELECT
sku
,code
,units
,single_deposit
,single_deposit * units AS total_deposit
,package_deposit

FROM base
