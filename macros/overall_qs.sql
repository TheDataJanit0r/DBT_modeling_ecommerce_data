{% macro overall_qs(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   id AS id,
   packaging_unit AS packaging_unit,
   no_of_base_units,
   base_unit_content,
   direct_shop_release,
   name,
   gtin_packaging_unit AS gfgh_gtin_packaging_unit,
   status,
   kollex_active,
   sku AS identifier,
   
   brand AS brand_and_title,
   title AS title,
   CONCAT(brand_and_title,' ',  amount_single_unit, 'x', net_content)::text as brand_title_content,
   base_code AS base_code,
   status_base AS status_base,
   pim_family AS family,
   manufacturer,
   net_content AS net_content,
   net_content_uom AS net_content_uom,
   amount_single_unit AS amount_single_unit,
   type_single_unit AS type_single_unit,
   type_packaging_unit AS type_packaging_unit,
   structure_packaging_unit AS structure_packaging_unit,
   release_l1 AS release_l1,
   shop_enabled AS shop_enabled,
   {{GFGH}}_enabled AS gfgh_enabled,
   gtin_single_unit AS gtin_single_unit,
   gtin_packaging_unit AS gtin_packaging_unit,
   l1_code AS l1_code,
   tool_link
from
   {{ref(GFGH)}} 
where status = 'enabled' and kollex_active = TRUE and {{GFGH}}_enabled = 'false' and coalesce(direct_shop_release, 'false') <> 'true' 


{% if not loop.last %} 
union 
{% endif %}

{% endfor %}


{% endmacro %}


