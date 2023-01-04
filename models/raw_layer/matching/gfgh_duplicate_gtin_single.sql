with gtins as (
select
    gtin_single_unit,
    json_agg(distinct identifier)::text as base_codes, 
    json_agg(distinct l1_code)::text as l1_codes, 
    json_agg(enablement)::text as enablement_across_merchants,
    json_agg(CONCAT(brand, ' ', title,' ',  amount_single_unit, 'x', net_content))::text as sku_title
from
   prod_raw_layer.all_skus
where
    gtin_single_unit is not null --and type_packaging_unit <> 'FREAK (abweichendes Design, Rücksprache halten)'
group by
    1
having
    count(*) > 1 and (json_array_length(json_agg(distinct l1_code)) > 1 or json_array_length(json_agg(distinct identifier)) > 1)
)

,agg as (select gtin_single_unit, identifier, l1_code, max(enablement) as enablement, max(CONCAT(brand, ' ', title,' ',  amount_single_unit, 'x', net_content))::text as title 
from prod_raw_layer.all_skus where gtin_single_unit in (select gtin_single_unit from gtins)
group by 1,2,3)



,final as (select gtin_single_unit, json_agg(identifier)::text as skus, 
    json_agg(l1_code)::text as l1_codes, 
    json_array_length(json_agg(distinct l1_code)) as no_of_l1_codes, 
    json_agg(enablement)::text as enablement_across_merchants,
    json_agg(title)::text as sku_title
    
from agg group by 1
union
select
    gtin_single_unit,
    json_agg(identifier)::text as skus,  
    json_agg(l1_code)::text as l1_codes, 
    json_array_length(json_agg(distinct l1_code)) as no_of_l1_codes, 
    json_agg(enablement)::text as enablement_across_merchants,
    json_agg(CONCAT(brand, ' ', title,' ',  amount_single_unit, 'x', net_content))::text as sku_title
from
    prod_raw_layer.all_skus
where
    gtin_single_unit is not null and amount_single_unit = '1' --and type_packaging_unit <> 'FREAK (abweichendes Design, Rücksprache halten)'
group by
    1
having
    count(*) > 1)

select * from final where no_of_l1_codes > 1



