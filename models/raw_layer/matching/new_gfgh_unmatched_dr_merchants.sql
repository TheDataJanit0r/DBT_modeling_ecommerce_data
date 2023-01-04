with base as ({{ ongoing_unmatched_dr_merchants(merchants_dr()) }})

select base.* from base
left join (select * from {{ref('new_gfgh_sku_match_dr_merchants')}}) x using (gfgh, id)
where x.id is null
