SELECT
   ma.info_provider_gln AS info_provider_gln,
   ma.gtin_single_unit AS gtin_single_unit,
   ma.gtin_packaging_unit AS gtin_packaging_unit,
   ma.liquid_code AS liquid_code,
   ma.family AS family,
   ma.gs1_short_desc AS gs1_short_desc,
   ma.gs1_desc AS gs1_desc,
   ma.amount AS amount,
   ma.amount_text AS amount_text,
   ma.brand AS brand,
   ma.mix AS mix,
   ma.trinkkontor_deposit AS trinkkontor_deposit,
   ma.trinkkontor_id AS trinkkontor_id,
   ma.trinkkontor_name AS trinkkontor_name,
   ma.trinkkontor_pkg AS trinkkontor_pkg,
   ma.gvs_id AS gvs_id,
   ma.gvs_name AS gvs_name,
   ma.gvs_pkg AS gvs_pkg,
   ma.krajewski_id AS krajewski_id,
   ma.krajewski_name AS krajewski_name,
   ma.krajewski_pkg AS krajewski_pkg,
   ma.roessler_id AS roessler_id,
   ma.roessler_pkg AS roessler_pkg,
   ma.roessler_name AS roessler_name,
   pm3.code AS base_same_liquid,
   p.identifier AS sku,
   pm.code AS parent,
   pm2.code AS grandparent
FROM
   (
      (
         (
            (
               (
                  (
                     {{ref('gs1_matches')}} ma
                     LEFT JOIN {{ref('pim_catalog_product')}} p ON COALESCE(
                        p.raw_values :: JSON -> 'gtin_packaging_unit' -> '<all_channels>' ->> '<all_locales>',
                        p.raw_values :: JSON -> 'gtin_single_unit' -> '<all_channels>' ->> '<all_locales>'
                     )::text = ma.gtin_packaging_unit::text
                  )
               )
            )
            LEFT JOIN {{ref('pim_catalog_product_model')}} pm ON((pm.id = p.product_model_id))
         )
         LEFT JOIN {{ref('pim_catalog_product_model')}} pm2 ON((pm2.id = pm.parent_id))
      )
      LEFT JOIN {{ref('pim_catalog_product_model')}} pm3 ON((pm3.code = ma.liquid_code))
   )