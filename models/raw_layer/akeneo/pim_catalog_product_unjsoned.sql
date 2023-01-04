{{
config({
    "post-hook": [
      "{{ index(this, 'identifier')}}",
      "{{ index(this, 'product_model_id')}}"
    ],
    })
}}

select
created,
identifier,
product_model_id,
_updated_at,
family_id,
raw_values::json,
coalesce(
        p.raw_values :: JSON -> 'shop_enabled' -> '<all_channels>' ->> '<all_locales>'::text,
        'false'
    ) AS shop_enabled,
p.raw_values :: JSON -> 'manufacturer_name' -> '<all_channels>' ->> '<all_locales>' as manufacturer_name,
p.raw_values :: JSON -> 'net_content' -> '<all_channels>' ->> '<all_locales>' as net_content,
p.raw_values :: JSON -> 'gtin_single_unit' -> '<all_channels>' ->> '<all_locales>' as gtin_single_unit,
p.raw_values :: JSON -> 'gtin_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as gtin_packaging_unit,
p.raw_values :: JSON -> 'duplicate' -> '<all_channels>' ->> '<all_locales>' as duplicate,
p.raw_values :: JSON -> 'title' -> '<all_channels>' ->> '<all_locales>' as title,
p.raw_values :: JSON -> 'foto_release_hash' -> '<all_channels>' ->> '<all_locales>'::text AS foto_release_hash,
p.raw_values :: JSON -> 'detail_type_single_unit' -> '<all_channels>' ->> '<all_locales>' as detail_type_single_unit,
p.raw_values :: JSON -> 'detail_type_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as detail_type_packaging_unit,
p.raw_values :: JSON -> 'net_content_liter' -> '<all_channels>' ->> '<all_locales>' as net_content_liter,
p.raw_values :: JSON -> 'net_content_2' -> '<all_channels>' ->> '<all_locales>' as net_content_2,
p.raw_values :: JSON -> 'net_content_uom' -> '<all_channels>' ->> '<all_locales>' as net_content_uom,
p.raw_values :: JSON -> 'amount_single_unit' -> '<all_channels>' ->> '<all_locales>' as amount_single_unit,
p.raw_values :: JSON -> 'type_single_unit' -> '<all_channels>' ->> '<all_locales>' as type_single_unit,
p.raw_values :: JSON -> 'type_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as type_packaging_unit,
p.raw_values :: JSON -> 'structure_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as structure_packaging_unit

from {{ref('pim_catalog_product')}} p

