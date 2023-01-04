{% macro ongoing_no_gtin_v2(GFGH_LIST) %}

with pt1 as (
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
   k.sku is null and k.direct_shop_release = 'false' and k.kollex_active = 'true' and k.status = 'enabled'

{% if not loop.last %}
union
{% endif %}

{% endfor %}
)
select * from pt1 where pt1.id not in (select gfgh_product_id from sheet_loader.special_cases_to_exclude) and gfgh not in (select merchant_key from sheet_loader.special_cases_to_exclude) and gfgh in {{GFGH_LIST}}
{% endmacro %}