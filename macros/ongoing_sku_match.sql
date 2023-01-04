
{% macro ongoing_sku_match(GFGH_LIST) %}

with pt1 as (
{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   s.id AS id,
   s.packaging_unit AS packaging_unit,
   s.no_of_base_units,
   s.base_unit_content,
   s.direct_shop_release,
   s.name AS name,
   s.manufacturer,
   s.gtin_packaging_unit AS gtin_packaging_unit,
   s.status AS status,
   s.kollex_active AS kollex_active,
   s.created AS created,
   s.updated AS updated,
   s.sku AS sku,
   a.identifier AS match_sku
   --s.tool_link
from

       {{ref(GFGH)}} s
      join
        prod_raw_layer.all_skus a
         on s.gpu = a.gtin_packaging_unit


where s.status = 'enabled' and s.kollex_active = true and s.sku is null  and coalesce(s.direct_shop_release, 'false') <> 'true'
{% if not loop.last %}
union
{% endif %}

{% endfor %}
)
select * from pt1 where pt1.id not in (select gfgh_product_id from sheet_loader.special_cases_to_exclude) and gfgh not in (select merchant_key from sheet_loader.special_cases_to_exclude) and gfgh in {{GFGH_LIST}}
{% endmacro %}

