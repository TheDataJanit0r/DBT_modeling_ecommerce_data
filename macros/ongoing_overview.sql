{% macro ongoing_overview(GFGH_LIST, prefix) %}

with 
    gfghs as (select distinct gfgh from {{ref( prefix+'gfgh_active')}} )
    ,sku_match as (select gfgh, count(*) as sku_match from {{ref(prefix+'gfgh_sku_match')}} group by 1)
    ,model_match as (select gfgh, count(*) as model_match from {{ref(prefix+'gfgh_model_match')}} group by 1)
    ,gs1_match as (select gfgh, count(*) as gs1_match from {{ref(prefix+'gfgh_gs1_match')}} group by 1)
    ,unmatched as (select gfgh, count(*) as unmatched from {{ref(prefix+'gfgh_unmatched')}} group by 1)
    ,no_gtin as (select gfgh, count(*) as no_gtin from {{ref(prefix+'gfgh_no_gtin')}} group by 1)
    ,no_sku as (select merchant_key as gfgh, count(*) as no_sku from {{ref('gfgh_data_no_sku')}} where merchant_key in {{ GFGH_LIST }} group by 1)
    ,qs as (select gfgh, count(*) as qs from {{ref(prefix+'gfgh_qs')}} group by 1)
    ,diff as (select gfgh_id as gfgh, count(*) as diff from {{ref('gfgh_import_diff_weekly')}} where gfgh_id in {{ GFGH_LIST }} and cal_week = to_char(current_date, 'YYYY-IW') group by 1)
    ,excluded as (select merchant_key as gfgh, count(*) as excluded from sheet_loader.special_cases_to_exclude where merchant_key in {{ GFGH_LIST }} group by 1)


select
    g.gfgh
    , COALESCE(sku_match.sku_match, 0) as sku_match
    , COALESCE(model_match.model_match, 0) as model_match
    , COALESCE(gs1_match.gs1_match, 0) as gs1_match
    , COALESCE(unmatched.unmatched, 0) as unmatched
    , COALESCE(no_gtin.no_gtin, 0) as no_gtin
    , COALESCE(no_sku.no_sku, 0) as no_sku
    , COALESCE(qs.qs, 0) as qs
    , COALESCE(diff.diff, 0) as diff
    , COALESCE(excluded.excluded, 0) as excluded

from gfghs g
left join sku_match using (gfgh)
left join model_match using (gfgh)
left join gs1_match using (gfgh)
left join unmatched using (gfgh)
left join no_gtin using (gfgh)
left join no_sku using (gfgh)
left join qs using (gfgh)
left join diff using (gfgh)
left join excluded using (gfgh)



{% endmacro %}

