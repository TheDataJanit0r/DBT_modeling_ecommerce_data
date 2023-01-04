with gms_merchants as (
	                      select uuid
	                      from fdw_customer_service.merchant
	                      where key in (
		                                   select merchant_key
		                                   from prod_raw_layer.holdings
		                                   where holding = 'tkk'
	                                   )
                      )

   , kollex_orders as (
	                      select *
	                      from fdw_shop_service.order
	                      where user_uuid is not null
                      )
select concat( all_skus.brand , '| ' , all_skus.amount_single_unit , ' x ' , all_skus.net_content_liter , ' Liter|' ,
               all_skus.sku )   as artikel
	 , sum( order_item.amount ) as verkaufsmenge
from gms_merchants
	     left join  kollex_orders on merchant_uuid = uuid

	     left join  fdw_shop_service.order_item on order_item.fk_order = kollex_orders.id_order


	     left join prod_raw_layer.all_skus
		                on all_skus.sku = replace( cast( order_item.order_item_meta -> 'sku' as text ) , '"' , '' )
where all_skus.brand is not null

group by 1
having sum( order_item.amount ) >0
order by 2 desc
limit 20
