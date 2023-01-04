with  akeneo as (
  select
    p.identifier as sku,
    p.raw_values :: json -> 'shop_enabled' -> '<all_channels>' ->> '<all_locales>' as shop_enabled,
    m.raw_values :: json -> 'golden_record_level1' -> '<all_channels>' ->> '<all_locales>' as release_l1,
    coalesce(
      m.raw_values :: json -> 'status_base' -> '<all_channels>' ->> '<all_locales>',
      m2.raw_values :: json -> 'status_base' -> '<all_channels>' ->> '<all_locales>'
    ) as status_base


      ,m2.updated dwh_received
    from from_pim.cp_pim_catalog_product p
    left join from_pim.cp_pim_catalog_product_model m on m.id = p.product_model_id
    left join (select
        id,
        raw_values,
        updated
      from
        from_pim.cp_pim_catalog_product_model
    ) m2 on m2.id = m.parent_id
),
gfgh_data as (
  select
    p.sku,
    gfgh_id as gfgh,
    p.active as active,
    p.gfgh_product_id as product_id,
    p.sales_unit_pkgg as packaging,
    p.name as product_name,
    p.qa as qa,
    p.direct_shop_release as boost,
    p.direct_sku as direct_sku,
    p.updated_at as dwh_received
  from
    from_pim.cp_gfgh_product p
)


select
  null as spryker_sku,
  g.active as spryker_active,
  a.dwh_received as spryker_updated_at
from akeneo a
  left join gfgh_data g on g.direct_sku::text = a.sku::text
where
  a.sku is null
  and g.direct_sku is null and char_length(a.sku) < 8
