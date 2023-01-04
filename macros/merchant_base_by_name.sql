{% macro merchant_base_by_name(GFGH_NAME) %}

with gfgh_base as
    (select
            *
     from {{ ref('gfgh_enrichment') }}
     where merchant_key = '{{ GFGH_NAME }}'
    )

,base as (SELECT           created_at              AS created
                           , gtin                    AS gpu
                           , CASE
                                 WHEN no_of_base_units = 1
                                    THEN gtin
                                 ELSE NULL
                              END                     AS gtin_single_unit
                           , CASE
                                 WHEN no_of_base_units > 1
                                    THEN gtin
                                 ELSE NULL
                              END                     AS gtin_packaging_unit
                           , no_of_base_units
                           , case when direct_shop_release = 1 then 'true' else 'false' end as direct_shop_release
                           , base_unit_content
                           , gfgh_product_id :: text AS id
                           , manufacturer
                           , name
                           , sales_unit_pkgg         AS packaging_unit
                           , updated_at              AS updated
                           , gfgh_product_id
                           , sales_unit_pkgg
                           , CASE
                                 WHEN active = 1
                                    THEN 'enabled'
                                 ELSE 'disabled'
                              END                     AS STATUS
                           , sku
                           , kollex_active
from
   from_pim.cp_gfgh_product p
where
   gfgh_id = '{{ GFGH_NAME }}'
)

select
   p.created
   , CASE
         WHEN p.gpu IS NOT NULL
            AND p.gpu :: text <> ''
            AND lpad( p.gpu :: text, 14, '0' ) <> '00000000000000'
            THEN lpad( p.gpu :: text, 14, '0' )
         ELSE NULL
      END AS               gpu
   , CASE
         WHEN p.gtin_single_unit IS NOT NULL
            AND lpad( p.gtin_single_unit :: text, 14, '0' ) <> '00000000000000'
            AND p.gtin_single_unit :: text <> ''
            THEN lpad( p.gtin_single_unit :: text, 14, '0' )
         ELSE NULL
      END AS               gtin_single_unit
   , CASE
         WHEN p.gtin_packaging_unit IS NOT NULL
            AND lpad( p.gtin_packaging_unit :: text, 14, '0' ) <> '00000000000000'
            AND p.gtin_packaging_unit :: text <> ''
            THEN lpad( p.gpu :: text, 14, '0' )
         ELSE NULL
      END AS               gtin_packaging_unit
   , p.id
   , p.no_of_base_units
   , p.base_unit_content
   , p.direct_shop_release
   , p.name
   , p.manufacturer
   , p.packaging_unit
   ,all_skus.brand
   ,all_skus.base_code
   ,all_skus.shop_enabled
   ,all_skus.structure_packaging_unit
   ,all_skus.type_packaging_unit
   ,all_skus.type_single_unit
   ,all_skus.amount_single_unit


   
   ,'' as tool_link
   ,all_skus.l1_code
   ,all_skus.{{GFGH_NAME}}_enabled
   ,all_skus.net_content_uom
   , p.updated
   , p.gfgh_product_id
   ,all_skus.name as title
   ,all_skus.release_l1 as release_l1
   ,concat(all_skus.name,' ',all_skus.brand) as brand_and_title
   ,all_skus.net_content
   ,all_skus.pim_family
   , CASE
         WHEN e.kollex_active::int IS NOT NULL
            THEN e.kollex_active::int
         ELSE p.kollex_active::int
      END::bool AS               kollex_active
   , CASE
         WHEN e.kollex_gtin_packaging_unit IS NOT NULL
            AND e.kollex_gtin_packaging_unit :: text <> ''
            THEN lpad( e.kollex_gtin_packaging_unit :: text, 14, '0' )
         ELSE NULL
      END AS               kollex_gtin_packaging_unit
   , p.sales_unit_pkgg
   , p.status
   , all_skus.status_base status_base
   , all_skus.status_base special_case

   , CASE
         WHEN COALESCE( p.sku, x.identifier ) <> ''
            THEN COALESCE( p.sku, x.identifier )
         ELSE NULL
      END AS               sku
from base p
left join gfgh_base e 
   ON p.gfgh_product_id::text = e.gfgh_product_id::text
left join from_pim.cp_pim_catalog_product x 
   ON x.raw_values::JSON->'gfgh_{{GFGH_NAME}}_id'->'<all_channels>'->>'<all_locales>' = p.id
 LEFT JOIN prod_raw_layer.all_skus 
   ON all_skus.identifier = p.sku


{% endmacro %}