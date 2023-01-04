{% macro merchant_enabled_with_sku(GFGH_NAME) %}

select
   k.id AS id,
   k.packaging_unit AS packaging_unit,
   k.name AS name,
   k.manufacturer,
   k.gpu,
   k.base_unit_content,
   k.no_of_base_units,
   k.direct_shop_release,
   k.gtin_single_unit,
   k.gtin_packaging_unit,
   k.status AS status,
   k.kollex_active AS kollex_active,
   k.created AS created,
   k.updated AS updated,
   k.kollex_gtin_packaging_unit,
   k.sku,
   CONCAT('https://kollex.de/intern/pim/stock/', '{{GFGH_NAME}}', '/',  COALESCE(k.sku, k.id)) as tool_link    
from {{ ref(GFGH_NAME) }} k 
where k.status = 'enabled' and k.kollex_active = TRUE
and special_case = FALSE
{% endmacro %}


--note that 1 merchant (roessler) have custom _enabled wit sku tables! changes need to be added there too
