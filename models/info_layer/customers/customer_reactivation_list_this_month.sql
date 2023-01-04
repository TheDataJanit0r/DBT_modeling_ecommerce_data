with row_numbers_calculation as (
	                                select *
		                                 , row_number( ) over (partition by id_customer order by dbt_valid_from) row_number
		                                 , lag( "Customer_reactivation_flag" )
		                                   over (partition by id_customer order by dbt_updated_at )               previous_value
		                                 , "Customer_reactivation_flag" as                                       current_value
		                                 , dbt_updated_at                 as                                       time_of_change
	                                from snapshots.customer_table_horeca_snapshot_flag
                                )

select *
from row_numbers_calculation
where previous_value is not null and date_trunc('month', time_of_change ) = date_trunc('month', current_date )
