with base as ({{ ongoing_unmatched(merchants_active()) }})

select base.* from base
left join (select * from {{ref('gfgh_sku_match')}}) x using (gfgh, id)
where x.id is null
