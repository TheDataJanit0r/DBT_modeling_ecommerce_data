select
    gtin_packaging_unit,
    json_agg(identifier)::text as skus, 
    json_agg(enablement)::text as enablement_across_merchants,
    json_agg(CONCAT(brand, ' ', title,' ',  amount_single_unit, 'x', net_content))::text as sku_title
from
    prod_raw_layer.all_skus
where
    gtin_packaging_unit is not null --and type_packaging_unit <> 'FREAK (abweichendes Design, Rücksprache halten)'
group by
    1
having
    count(*) > 1
