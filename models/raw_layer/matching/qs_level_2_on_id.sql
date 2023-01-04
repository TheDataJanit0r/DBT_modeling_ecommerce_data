-- select
--    p.identifier AS identifier,
--    p.raw_values::JSON->'gtin_packaging_unit'->'<all_channels>'->>'<all_locales>' AS gtin_packaging_unit,
--    v1.value AS amount_single_unit,
--    gs1.amount AS gs1_amount,
--    CONCAT(s.brand, ' ', s.title) AS brand_and_title,
--    gs1.amount_text AS gs1_amount_text,
--    v2.value AS structure_packaging_unit,
--    v4.value AS type_packaging_unit,
--    gs1.description AS gs1_desc,
--    gs1.short_description AS gs1_short_desc,
--    gs1.data::JSON->'"5 - Verpackung   Logistik   Pre"'->'WSCEPackagingInformation[0]'->>'packagingTypeCode' AS gs1_packaging,
--    coalesce(pm.raw_values::JSON->'title'->'<all_channels>'->>'<all_locales>', pm2.raw_values::JSON->'title'->'<all_channels>'->>'<all_locales>') AS title,
--    v3.value AS net_content,
--
--    {% for GFGH in  merchants_all() %}
--    case when coalesce(p.raw_values::JSON -> 'gfgh_{{GFGH}}_id' -> '<all_channels>' ->> '<all_locales>', '') <> '' then TRUE else FALSE end as {{GFGH}},
--    {% endfor %}
--
--    ao8.value AS status_base,
--    pm.raw_values::JSON->'golden_record_level1'->'<all_channels>'->>'<all_locales>' AS gr_l1,
--    (gs1.data::JSON->'"0 - Identifizierung   Klassifiz"'->'IsTradeItemABaseUnit')::text AS gs1_base_unit
-- from
--    (
-- ((((((((((( {{ ref('pim_catalog_product') }} p
--
--       left join prod_raw_layer.all_skus s using (identifier)
--       left join
--          {{ref('pim_catalog_product_model')}} pm
--          on((pm.id = p.product_model_id)))
--       left join
--          {{ref('pim_catalog_product_model')}} pm2
--          on((pm2.id = pm.parent_id)))
--       left join
--          {{ ref('gs1data') }} gs1
--          on((gs1.gtin_packaging_unit = p.raw_values::JSON->'gtin_packaging_unit'->'<all_channels>'->>'<all_locales>')))
--       left join
--          {{ref('pim_catalog_attribute_option')}} o1
--          on(((o1.code = p.raw_values::JSON->'amount_single_unit'->'<all_channels>'->>'<all_locales>')
--          and
--          (
--             o1.attribute_id = 20
--          )
-- )))
--       left join
--          {{ref('pim_catalog_attribute_option_value')}} v1
--          on((v1.option_id = o1.id)))
--       left join
--          {{ref('pim_catalog_attribute_option')}} o2
--          on(((o2.code = p.raw_values::JSON->'structure_packaging_unit'->'<all_channels>'->>'<all_locales>')
--          and
--          (
--             o2.attribute_id = 30
--          )
-- )))
--       left join
--          {{ref('pim_catalog_attribute_option_value')}} v2
--          on((v2.option_id = o2.id)))
--       left join
--          {{ref('pim_catalog_attribute_option')}} o3
--          on(((o3.code = pm.raw_values::JSON->'net_content_liter'->'<all_channels>'->>'<all_locales>')
--          and
--          (
--             o3.attribute_id = 36
--          )
-- )))
--       left join
--          {{ref('pim_catalog_attribute_option_value')}} v3
--          on((v3.option_id = o3.id)))
--       left join
--          {{ref('pim_catalog_attribute_option')}} o4
--          on(((o4.code = p.raw_values::JSON->'type_packaging_unit'->'<all_channels>'->>'<all_locales>')
--          and
--          (
--             o4.attribute_id = 32
--          )
-- )))
--       left join
--          {{ref('pim_catalog_attribute_option_value')}} v4
--          on((v4.option_id = o4.id)))
--       left join
--          {{ref('attribute_options')}} ao8
--          on(((ao8.attribute_code = 'status_base')
--          and
--          (
--             ao8.option_code = coalesce(pm.raw_values::JSON->'status_base'->'<all_channels>'->>'<all_locales>',
--                 pm2.raw_values::JSON->'status_base'->'<all_channels>'->>'<all_locales>')
--          )
-- ))
--    )
-- where
--    (
-- (pm.raw_values::JSON->'golden_record_level1'->'<all_channels>'->>'<all_locales>' = 'true')
--       and
--       (
--          p.raw_values::JSON->'shop_enabled'->'<all_channels>'->>'<all_locales>' is null
--
--          or
--          (
--             p.raw_values::JSON->'shop_enabled'->'<all_channels>'->>'<all_locales>' = 'false'
--
--          )
--       )
--       and
--       (
--          ao8.value = 'Freigegeben'
--       )
--       and
--       (
--         p.raw_values::JSON->'gfgh_enabled'->'<all_channels>'->>'<all_locales>' like '%rue%'
--       )
--    )

select distinct a.identifier,
         a.gtin_packaging_unit,
         a.amount_single_unit,
         b.amount,
         a.brand,
         a.title,
         b.amount_text,
         a.structure_packaging_unit,
         a.type_packaging_unit,
         b.description as gs1_desc,
         b.short_description as gs1_short_desc,
         a.net_content_liter as net_content,
         {% for GFGH in  merchants_new() %}
            {%if not 'test' in GFGH%}
                case when {{GFGH}}_enabled LIKE '%rue%' Then 'True' ELSE 'False' END AS {{GFGH}}_enabled
                {% if not loop.last %}
                    ,
                {%endif%}
            {%endif%}
        {% endfor %}

         from prod_raw_layer.all_skus as a
         left join from_pim.cp_gs1data as b on a.gtin_packaging_unit = b.gtin_packaging_unit

         where a.status_base = 'Freigegeben'
             and
         a.release_l1 like '%rue%'
             and
         (a.shop_enabled IS NULL
             OR
         a.shop_enabled = 'false')
             and
         (
            {% for GFGH2 in  merchants_new() %}
               {%if not 'test' in GFGH2%}
                    a.{{GFGH2}}_enabled like '%rue%'
                    {% if not loop.last %}
                        or
                    {%endif%}
               {%endif%}
            {% endfor %}
         )
