select count(distinct id_order)
from fdw_shop_service."order"
left join fdw_shop_service.order_item on "order".id_order=  order_item.fk_order
left join fdw_shop_service.product on order_item.fk_product = product.id_product
left join prod_raw_layer.all_skus on product.direct_sku = all_skus.identifier
left join prod_info_layer.jv_manufacturers on all_skus.manufacturer_name = jv_manufacturers.manufacturer_name

where c = true and "order".created_at > '2022-01-01'::date