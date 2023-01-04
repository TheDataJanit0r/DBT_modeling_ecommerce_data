


select count( distinct "order".id_order )

	 , extract( year from "order".created_at )  as Year
	 
from fdw_shop_service."order"
	     left join fdw_shop_service."order_item"
		               on "order_item".fk_order = "order".id_order
	     left join fdw_shop_service.product
		               on order_item.fk_product = product.id_product
	     left join from_pim.cp_gfgh_product
		               on cp_gfgh_product.direct_sku::text = product.direct_sku::text
	     left join prod_raw_layer.all_skus
		               on all_skus.identifier::text = cp_gfgh_product.direct_sku::text
where ( product.name like '%ola%'
	or product.name like '%oca%'
	or product.name like '%polli%'
	or product.name like '%onster%'
	or product.name like '%odenthal%'
	or all_skus.manufacturer like '%ola%'
	or all_skus.manufacturer like '%oca%'
	or all_skus.manufacturer like '%polli%'
	or all_skus.manufacturer like '%onster%'
	or all_skus.manufacturer like '%odenthal%'
	or all_skus.manufacturer_name like '%ola%'
	or all_skus.manufacturer_name like '%oca%'
	or all_skus.manufacturer_name like '%polli%'
	or all_skus.manufacturer_name like '%onster%'
	or all_skus.manufacturer_name like '%odenthal%' )
and "order".created_at >= '01-01-2017'

group by  extract( year from "order".created_at )
order by extract( year from "order".created_at ) asc
