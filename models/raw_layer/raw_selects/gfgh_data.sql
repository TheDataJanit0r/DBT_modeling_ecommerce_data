{{
    config(
        pre_hook=[
            "update from_pim.cp_gfgh_product set gfgh_product_id = '60801' where gfgh_product_id like '60801%' and gfgh_id = 'petersenundsohn'",
            "update from_pim.cp_gfgh_product set gfgh_product_id = '60800' where gfgh_product_id like '60800%' and gfgh_id = 'petersenundsohn'",
            "update from_pim.cp_gfgh_product set gfgh_product_id = '60799' where gfgh_product_id like '60799%' and gfgh_id = 'petersenundsohn'",
            "update from_pim.cp_gfgh_product set gfgh_product_id = '4011' where gfgh_product_id like '4011%' and gfgh_id = 'getraenkeblitz'",
            "update from_pim.cp_gfgh_product set gfgh_product_id = '4012' where gfgh_product_id like '4012%' and gfgh_id = 'getraenkeblitz'"
        ]
    )
}}

select
p.id,
p.sku::text,
p.base_unit_content::numeric,
p.base_unit_content_uom::text,
p.no_of_base_units,
p.gtin::text,
p.kollex_product_id::text,
p.manufacturer::text,
p.manufacturer_gln::text,
p.manufacturer_id::text,
p.flags::text,
p.list_price::numeric,
p.refund_value::text,
p.created_at::timestamp,
p.updated_at::timestamp,
p.gfgh_product_id::text,
p.sales_unit_pkgg::text,
p.name::text,
p.category_code::text,
p.direct_sku::text,

case when p.direct_shop_release = 1 then TRUE
     when p.direct_shop_release = 0 then FALSE else NULL end as direct_shop_release,

case when p.kollex_active = 1 then TRUE
     when p.kollex_active = 0 then FALSE else NULL end as kollex_active,

case when p.active = 1 then TRUE
     when p.active = 0 then FALSE else NULL end as active,

case when p.qa = 1 then TRUE
     when p.qa = 0 then FALSE else NULL end as qa,

case when p.was_direct_release = 1 then TRUE
     when p.was_direct_release = 0 then FALSE else NULL end as was_direct_release,

p.predicted_category::text, c.category_predicted, p.gfgh_id as merchant_key, case when x.gfgh_product_id is not null then TRUE else FALSE end as special_case
,p._updated_at::timestamp as _sdc_extracted_at
from
   from_pim.cp_gfgh_product p
left join sheet_loader.special_cases_to_exclude x on p.gfgh_product_id::text = x.gfgh_product_id::text and x.merchant_key = p.gfgh_id
left join prod_raw_layer.gfgh_data_predicted_categories c on p.id::text = c.id::text
