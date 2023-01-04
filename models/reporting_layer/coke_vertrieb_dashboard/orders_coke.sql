select order_number                               order_no

	 , to_char( "order".created_at , 'YYYYMMDD' ) date_at
	 , NULL                 as                    "Vorgang"
	 , merchant.key
	 , 'Nicht vorhanden'    as                    merchant_gedat_id

	 , 'nicht Vorhanden'                          gedat_id


	 , order_item.amount
	 , product_packaging_unit
	 , merchant.name
	 , address.street       as                    "merchant_street"
	 , address.house_number as                    "merchant_housenumber"
	 , address.zip          as                    "merchant_zip"
	 , address.city         as                    "merchant_city"
	 , "Street"             as                    "customer_street"
	 , "City"               as                    "customer_city"
	 , "Zip Code"           as                    "zip_customer"
	 , order_item.product_name

from fdw_shop_service."order"

	     left join  fdw_customer_service.merchant on merchant_uuid = merchant.uuid
	     left join  fdw_shop_service.order_item on "order".id_order = fk_order
	     inner join prod_info_layer.customer_table_horeca_children_customers_classified cth
		                on cth."UUID" = "order".customer_uuid
	     inner join fdw_customer_service.address
		                on address.id_address = merchant.fk_address

where "Coke Kunde" = 'true'
  and user_uuid is not null
  and address.street not like '%Test%'
  and "Customer Email" not like '%Test%'
order by 1, 2