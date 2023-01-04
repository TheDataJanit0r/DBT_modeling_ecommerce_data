{{
config({
    "post-hook": [
      "{{ index(this, 'id')}}",
      "{{ index(this, 'parent_id')}}"
    ],
    })
}}

select
id,
parent_id,
code,
_updated_at,
coalesce(
        raw_values :: JSON -> 'shop_enabled' -> '<all_channels>' ->> '<all_locales>'::text,
        'false'
    ) AS shop_enabled,
raw_values :: JSON -> 'manufacturer_name' -> '<all_channels>' ->> '<all_locales>' as manufacturer_name,

raw_values :: JSON -> 'contact_info' -> '<all_channels>' ->> '<all_locales>' as contact_info,
raw_values :: JSON -> 'gs1_contact_info' -> '<all_channels>' ->> '<all_locales>' as gs1_contact_info,

raw_values :: JSON -> 'net_content' -> '<all_channels>' ->> '<all_locales>' as net_content,
raw_values :: JSON -> 'gtin_single_unit' -> '<all_channels>' ->> '<all_locales>' as gtin_single_unit,
raw_values :: JSON -> 'gtin_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as gtin_packaging_unit,
raw_values :: JSON -> 'duplicate' -> '<all_channels>' ->> '<all_locales>' as duplicate,


raw_values :: JSON -> 'title' -> '<all_channels>' ->> '<all_locales>' as title,
raw_values :: JSON -> 'detail_type_single_unit' -> '<all_channels>' ->> '<all_locales>' as detail_type_single_unit,
raw_values :: JSON -> 'detail_type_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as detail_type_packaging_unit,

raw_values :: JSON -> 'net_content_liter' -> '<all_channels>' ->> '<all_locales>' as net_content_liter,
raw_values :: JSON -> 'net_content_2' -> '<all_channels>' ->> '<all_locales>' as net_content_2,
raw_values :: JSON -> 'net_content_uom' -> '<all_channels>' ->> '<all_locales>' as net_content_uom,
raw_values :: JSON -> 'amount_single_unit' -> '<all_channels>' ->> '<all_locales>' as amount_single_unit,
raw_values :: JSON -> 'type_single_unit' -> '<all_channels>' ->> '<all_locales>' as type_single_unit,
raw_values :: JSON -> 'type_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as type_packaging_unit,
raw_values :: JSON -> 'structure_packaging_unit' -> '<all_channels>' ->> '<all_locales>' as structure_packaging_unit,

raw_values :: JSON -> 'golden_record_level1' -> '<all_channels>' ->> '<all_locales>' as golden_record_level1,
raw_values :: JSON -> 'status_base' -> '<all_channels>' ->> '<all_locales>' as status_base,
raw_values :: JSON -> 'brand' -> '<all_channels>' ->> '<all_locales>' as brand

from {{ref('pim_catalog_product_model')}} 

