with
    base as ({{ ongoing_unmatched(merchants_new()) }}),
    pt1 as (
        select base.* from base
        left join (select * from {{ref('new_gfgh_sku_match')}}) x using (gfgh, id)
        where x.id is null
    )

select *
from pt1
where
    pt1.id not in (
        select gfgh_product_id
        from sheet_loader.special_cases_to_exclude
    ) and
    gfgh not in (select merchant_key from sheet_loader.special_cases_to_exclude)
