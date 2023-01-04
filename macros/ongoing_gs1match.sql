{% macro ongoing_gs1match(GFGH_LIST) %}
with exclude as (

   SELECT id,gfgh from {{ref('gfgh_sku_match')}}
   union
   SELECT id,gfgh from {{ref('new_gfgh_sku_match')}}
   union
   SELECT id,gfgh from {{ref('gfgh_model_match')}}
   union
   SELECT id,gfgh from {{ref('new_gfgh_model_match')}}
)
, pt1 as (
{% for GFGH in GFGH_LIST %}
select
   '{{GFGH}}' AS gfgh,
   g.info_provider_gln AS info_provider_gln,
   g.gtin_single_unit AS gtin_single_unit,
   g.gtin_packaging_unit AS gtin_packaging_unit,
   g.liquid_code AS liquid_code,
   g.family AS family,
   g.short_description AS gs1_short_desc,
   g.description AS gs1_desc,
   g.amount AS amount,
   g.amount_text AS amount_text,
   g.brand AS brand,
   g.mix AS mix,
   k.id AS gfgh_id,
   k.no_of_base_units,
   k.base_unit_content,
   k.direct_shop_release,
   k.packaging_unit AS gfgh_pkg,
   k.name AS gfgh_name,
   k.manufacturer,
   k.sku AS sku,
   k.tool_link
from {{ ref('gs1data') }} g
join {{ref(GFGH)}} k on k.gpu = g.gtin_packaging_unit and k.kollex_active = TRUE and coalesce(k.direct_shop_release, 'false') <> 'true'
left join exclude on k.id = exclude.id and exclude.gfgh = '{{GFGH}}'
where k.sku is null and exclude.id is null

{% if not loop.last %}
union
{% endif %}

{% endfor %}
)
select * from pt1 where pt1.gfgh_id not in (select gfgh_product_id from sheet_loader.special_cases_to_exclude) and gfgh not in (select merchant_key from sheet_loader.special_cases_to_exclude)
{% endmacro %}

