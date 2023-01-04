

with ids as (select kxe_id, "Kategorie" as cat_name from sheet_loader.kxe_categories)

, food_mapped_ids as (select kxe_id, cat_name, coalesce(brick_id, class_id, family_id) mapped_gs1_id, family_id, class_id, brick_id 
from ids
left join sheet_loader.gs1_german_family using (kxe_id)
left join sheet_loader.gs1_german_class using (kxe_id)
left join sheet_loader.gs1_german_brick using (kxe_id)
where kxe_id like '2-%'
)
,final as(
select liquid_code, description, short_description, gtin_packaging_unit, gtin_single_unit, brand, g.family_id as mapped_gs1_id, i.family_id, i.class_id, i.brick_id from {{ref('all_gs1_items_categories')}} g
join food_mapped_ids i using (family_id)
union

select liquid_code, description, short_description, gtin_packaging_unit, gtin_single_unit, brand, g.brick_id, i.family_id, i.class_id, i.brick_id from {{ref('all_gs1_items_categories')}} g
join food_mapped_ids i using (brick_id)
union

select liquid_code, description, short_description, gtin_packaging_unit, gtin_single_unit, brand, g.class_id, i.family_id, i.class_id, i.brick_id from {{ref('all_gs1_items_categories')}} g
join food_mapped_ids i using (class_id)
)

,final_final as (select liquid_code, description, short_description, gtin_packaging_unit, gtin_single_unit, brand, max(mapped_gs1_id) mapped_gs1_id 
from final where description is not null or short_description is not null
group by 1,2,3,4,5,6)

,everything as (
select liquid_code, description, short_description, gtin_packaging_unit, gtin_single_unit, brand, mapped_gs1_id, cat_name 

from final_final f
join food_mapped_ids g using (mapped_gs1_id) 
)

select distinct * from everything
