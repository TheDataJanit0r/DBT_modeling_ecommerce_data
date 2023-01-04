select id
	 , gtin
	 , name
	 , flags
	 , active::int::boolean
	 , import_id
	 , created_at::timestamp as created_at
	 , list_price
	 , updated_at::timestamp as updated_at
	 , manufacturer
	 , refund_value
	 , gfgh_product_id
	 , sales_unit_pkgg
	 , manufacturer_gln
	 , no_of_base_units
	 , base_unit_content
	 , kollex_product_id
	 , base_unit_content_uom
from from_pim.cp_gfgh_product_import