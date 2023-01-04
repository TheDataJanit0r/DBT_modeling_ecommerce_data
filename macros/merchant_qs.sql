{% macro merchant_qs(GFGH_NAME) %}

select
   k.id AS id,
   k.packaging_unit AS packaging_unit,
   k.name AS name,
   k.no_of_base_units,
   k.base_unit_content,
   k.direct_shop_release,
   k.gtin_packaging_unit AS gfgh_gtin_packaging_unit,
   k.status AS status,
   k.kollex_active AS kollex_active,
   s.identifier AS identifier,
   s.brand as brand,
   CONCAT(s.brand, ' ', s.title) AS brand_and_title,
   s.title AS title,
   s.{{GFGH_NAME}}_id AS {{GFGH_NAME}},
  -- s.base_code AS base_code,
   s.status_base AS status_base,
   s.pim_family AS family,
   coalesce(s.manufacturer_name, k.manufacturer) manufacturer,
   s.net_content AS net_content,
   s.net_content_uom AS net_content_uom,
   s.amount_single_unit AS amount_single_unit,
   s.type_single_unit AS type_single_unit,
   s.type_packaging_unit AS type_packaging_unit,
   s.structure_packaging_unit AS structure_packaging_unit,
   s.release_l1 AS release_l1,
   s.shop_enabled AS shop_enabled,
   s.{{GFGH_NAME}}_enabled AS {{GFGH_NAME}}_enabled,
   s.gtin_single_unit AS gtin_single_unit,
   s.gtin_packaging_unit AS gtin_packaging_unit,
--   s.l1_code AS l1_code,
   special_case,
   CONCAT('https://kollex.de/intern/pim/stock/', '{{GFGH_NAME}}', '/',  COALESCE(s.identifier, k.id)) as tool_link    
from {{ ref(GFGH_NAME) }} k 
left join prod_raw_layer.all_skus s on s.{{GFGH_NAME}}_id = k.id


{% endmacro %}


