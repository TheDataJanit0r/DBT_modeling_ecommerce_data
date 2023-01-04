{% macro merchant_base(GFGH_ID) %}

with gfgh_base as (select * from {{ ref('gfgh_enrichment') }} where gfgh_id = {{ GFGH_ID }} ),

base as (select
   created_at as created,
   gtin as gpu,
   case when no_of_base_units = 1 then gtin else NULL END as gtin_single_unit, 
   case when no_of_base_units > 1 then gtin else NULL END as gtin_packaging_unit, 
   no_of_base_units,
   direct_shop_release,
   base_unit_content,
   gfgh_product_id::text as id,
   name,
   sales_unit_pkgg as packaging_unit,
   updated_at as updated,
   gfgh_product_id,
   sales_unit_pkgg,
   case when active = true then 'enabled' else 'disabled' end as status,
   sku,
   kollex_active,
   _sdc_extracted_at, special_case
from
   {{ ref('gfgh_data') }} 
where
   gfgh_id = {{ GFGH_ID }}
)

select 
   p._sdc_extracted_at,
   p.created,
   case when p.gpu is not null and p.gpu::text <> '' and lpad(p.gpu::text,14,'0') <> '00000000000000' then lpad(p.gpu::text,14,'0') else NULL end as gpu,
   case when p.gtin_single_unit is not null and lpad(p.gtin_single_unit::text,14,'0') <> '00000000000000' and p.gtin_single_unit::text <> '' then lpad(p.gtin_single_unit::text,14,'0') else NULL end as gtin_single_unit,
   case when p.gtin_packaging_unit is not null and lpad(p.gtin_packaging_unit::text,14,'0') <> '00000000000000' and p.gtin_packaging_unit::text <> '' then lpad(p.gpu::text,14,'0') else NULL end as gtin_packaging_unit,
   p.id,
   p.no_of_base_units,
   p.base_unit_content,
   p.direct_shop_release,
   p.name,
   p.packaging_unit,
   p.updated,
   p.gfgh_product_id,
   case when e.kollex_active is not null then e.kollex_active else p.kollex_active end as kollex_active,
   case when e.kollex_gtin_packaging_unit is not null and e.kollex_gtin_packaging_unit::text <> '' then lpad(e.kollex_gtin_packaging_unit::text,14,'0') else NULL end as kollex_gtin_packaging_unit,
   p.sales_unit_pkgg,
   p.status,
   p.sku, 
   special_case  

from base p
left join gfgh_base e on p.gfgh_product_id::text = e.gfgh_product_id::text

{% endmacro %}
