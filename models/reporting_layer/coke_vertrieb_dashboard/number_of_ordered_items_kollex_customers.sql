


select sum(  "order_item".amount )

	 , extract( year from "order".created_at )  as Year
	 , extract( month from "order".created_at )  as Month
	 ,max(concat(extract( year from "order".created_at ) ,'-'
	 , extract( month from "order".created_at ))) Calender_Week
from fdw_shop_service."order"
	     left join fdw_shop_service."order_item"
		               on "order_item".fk_order = "order".id_order

where

    "order".created_at >= '01-01-2022'

group by  extract( month from "order".created_at ),extract( year from "order".created_at )
order by extract( year from "order".created_at ) asc,extract( year from "order".created_at )
