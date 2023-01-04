{% macro ongoing_no_gtin_dr_merchants(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   k.id AS id,
   k.no_of_base_units,
   k.base_unit_content,
   k.direct_shop_release,
   k.packaging_unit AS packaging_unit,
   k.manufacturer,
   k.name AS name,
   k.kollex_active AS kollex_active,
   k.tool_link
from
   {{ref(GFGH)}} k
where
   k.sku is null and k.gpu is null and coalesce(k.direct_shop_release, 'false') like '%rue%' and k.kollex_active=true

{% if not loop.last %}
union
{% endif %}

{% endfor %}

{% endmacro %}

