with customer_with_parents as (
	                              with a as (
		                                        select max( customer.email )       as        "Horeca Email"
			                                         , max( customer.name )        as        "Horeca Name"

			                                         , 'KIP'                       as        app

			                                         , max( address.street )                 "address"
			                                         , max( address.house_number ) as        house_number
			                                         , max( address.zip )                    "Zip Code"
			                                         , max( address.city )                   "City"

			                                         , min( customer_has_parent.created_at ) "registration date"
			                                         , customer.id_customer                  "id_customer"
			                                         , max( customer_has_merchant.status )   "activated by merchant"

			                                         , max( merchant.key )                   "GFGH"
			                                         , max( contact.mobile_phone )           mobile_phone
			                                         , min( customer_invitation.created_at ) "customer_invitation_date"





		                                        from fdw_customer_service.customer


			                                             left join fdw_customer_service.address
				                                                       on address.id_address = customer.fk_address
			                                             left join fdw_customer_service.customer_has_merchant
				                                                       on customer.id_customer = customer_has_merchant.fk_customer
			                                             left join fdw_customer_service.merchant
				                                                       on customer_has_merchant.fk_merchant = merchant.id_merchant
			                                             left join fdw_customer_service.customer_has_contacts
				                                                       on customer.id_customer = customer_has_contacts.fk_customer
			                                             left join fdw_customer_service.contact
				                                                       on customer_has_contacts.fk_contact = contact.id_contact
			                                             left join fdw_customer_service.customer_invitation
				                                                       on customer_has_merchant.id_customer_has_merchant =
				                                                          customer_invitation.fk_customer_merchant
			                                             left join fdw_customer_service.customer_has_parent
				                                                       on customer.id_customer = customer_has_parent.fk_customer


		                                        where customer.name not like '%Test%'
--                                             and coalesce(customer.email, "user".email) not like '%Test%'
			                                      and customer.name not like '%test%'
--                                             and coalesce(customer.email, "user".email) not like '%test%'
			                                      and customer.name not like '%kollex%'
--                                             and coalesce(customer.email, "user".email) not like '%kollex%'
			                                      and customer.id_customer not in (
				                                                                      select distinct fk_customer
				                                                                      from fdw_customer_service.customer_has_user_and_role
			                                                                      )
		                                        group by customer.id_customer
	                                        )

		                             , parent_phone_numbers as (
			                                                       select max( "user".mobile_phone ) as mobile_phone_parent
				                                                        , max( "user".email )        as email_parent
				                                                        , max( street )              as street_parent
				                                                        , max( zip )                    zip_parent
				                                                        , max( city )                   city_parent
				                                                        , max( house_number )           house_number_parent
				                                                        , customer_has_parent.fk_customer
			                                                       from fdw_customer_service.customer_has_user_and_role
				                                                            left join fdw_customer_service.customer_has_parent
					                                                                      on customer_has_user_and_role.fk_customer =
					                                                                         customer_has_parent.fk_customer_parent
				                                                            left join fdw_customer_service."user"
					                                                                      on "user".id_user = customer_has_user_and_role.fk_user
				                                                            left join fdw_customer_service.customer_has_business_unit
					                                                                      on customer_has_user_and_role.fk_customer =
					                                                                         customer_has_business_unit.fk_customer
				                                                            left join fdw_customer_service.business_unit
					                                                                      on customer_has_business_unit.fk_business_unit =
					                                                                         business_unit.id_business_unit
				                                                            left join fdw_customer_service.address
					                                                                      on business_unit.fk_address = address.id_address
			                                                       group by customer_has_parent.fk_customer
		                                                       )

	                              select a.id_customer
		                               , coalesce( a."Horeca Email" , parent_phone_numbers.email_parent ) "Horeca Email"
		                               , "Horeca Name"
		                               , app
		                               , coalesce( address , street_parent )                              address
		                               , coalesce(house_number,house_number_parent) house_number
		                               , coalesce("Zip Code",zip_parent) "Zip Code"
		                               , coalesce("City",city_parent) "City"
		                               , "registration date"

		                               , "activated by merchant"
		                               , "GFGH"
		                               , customer_invitation_date
		                               , uuid                                                             "UUID"
		                               , coalesce( parent_phone_numbers.mobile_phone_parent::text ,
		                                           a.mobile_phone::text ) as                              mobile_phone
	                              from a
		                                   left join fdw_customer_service.customer on customer.id_customer = a.id_customer
		                                   left join parent_phone_numbers
			                                             on parent_phone_numbers.fk_customer = a.id_customer
                              )


   , consolidated_list_of_customers as (
	                                       select "Horeca Email"
			                                    , "Horeca Name"
			                                    , app
			                                    , "UUID"
			                                    , address
			                                    , house_number
			                                    , "Zip Code"
			                                    , "City"
			                                    , "registration date"
			                                    , id_customer
			                                    , "activated by merchant"
			                                    , "GFGH"
			                                    , 'has Parent/active' "active"
			                                    , customer_invitation_date
			                                    , mobile_phone
	                                       from customer_with_parents
                                       )


-- here getting the order details for first and last order to calculate status in the calculated status CTE
   , order_details as (
	                      select max( "order".created_at ) as "Last Order"
			                   , min( "order".created_at ) as "First Order"

			                   , max( "order".status )     as "Order Status"
			                   -- , "Company ID"
			                   , max( c."Horeca Email" )   as "Horeca Email"
			                   -- , max( c."Horeca Name" )    as "Horeca Name"

			                   , count( "order".id_order ) as "Number Of Orders"
			                   , c.id_customer
	                      from consolidated_list_of_customers c
		                           left join fdw_shop_service.order
			                                     on c."UUID"::text = "order".customer_uuid::text
	                      where "order".user_uuid is not null

	                      group by c.id_customer
                      )
   , order_details_last_3_months as (
	                                    select max( "order".created_at ) as "Last Order last 3 months"
			                                 , min( "order".created_at ) as "First Order last 3 months"
			                                 , count( "order".id_order ) as "Number Of Orders last 3 months"
			                                 , c.id_customer
	                                    from consolidated_list_of_customers c
		                                         left join fdw_shop_service.order
			                                                   on c."UUID"::text = "order".customer_uuid::text


	                                    where "order".created_at > now( ) - '90 day'::interval
			                              and "order".user_uuid is not null
	                                    group by c.id_customer
                                    )
   , calculating_average_orders_per_customers as (
	                                                 select "Last Order"
			                                              , "First Order"
			                                              , "Order Status"
			                                              , "Horeca Email"
			                                              , "Number Of Orders"
			                                              , order_details.id_customer
			                                              , "Last Order last 3 months"
			                                              , "First Order last 3 months"
			                                              , "Number Of Orders last 3 months"
			                                              , extract( day from ( now( ) - "First Order"::timestamp ) )
			                                              , case
				                                                when "Number Of Orders" > 0
					                                                then ceil(
							                                                EXTRACT( day from ( now( ) - "First Order"::timestamp ) ) /
							                                                "Number Of Orders" )
				                                                else 0
			                                                end      "average days between orders"
			                                              , case
				                                                when "Number Of Orders last 3 months" > 0 and
				                                                     EXTRACT( day from
				                                                              ( "Last Order last 3 months"::timestamp -
				                                                                "First Order last 3 months"::timestamp ) ) >
				                                                     0
					                                                then ceil(
							                                                EXTRACT( day from
							                                                         ( "Last Order last 3 months"::timestamp -
							                                                           "First Order last 3 months"::timestamp ) ) /
							                                                "Number Of Orders last 3 months" )
				                                                else 0
			                                                end::int "average days between orders last 3 months"


	                                                 from order_details
		                                                      left join order_details_last_3_months
			                                                                on order_details.id_customer = order_details_last_3_months.id_customer
                                                 )

   , calculating_reactivation_flag as (
	                                      select case
		                                             when NOW( ) -
		                                                  make_interval( days=>"average days between orders last 3 months"::int ) >
		                                                  "Last Order" and
		                                                  "average days between orders last 3 months" > 0
			                                             then true
		                                             else
			                                             false
	                                             end "Customer_reactivation_flag"
			                                   , "Last Order"
			                                   , "First Order"
			                                   , "Order Status"
			                                   , "Horeca Email"
			                                   , "Number Of Orders"
			                                   , id_customer
			                                   , "Last Order last 3 months"
			                                   , "First Order last 3 months"
			                                   , "Number Of Orders last 3 months"
			                                   , "average days between orders"
			                                   , "average days between orders last 3 months"

	                                      from calculating_average_orders_per_customers
                                      )

-- this is only for shop customers, because the merchant has to activate each Horeca
   , last_successful_supplier_request as (
	                                         select consolidated_list_of_customers."id_customer"
			                                      , max( document_upload.created_at ) "Last Successful List Upload"
	                                         from fdw_customer_service.document_upload
		                                              left join fdw_customer_service.business_unit
			                                                        on business_unit.id_business_unit =
			                                                           document_upload.fk_customer_business_unit
		                                              left join fdw_customer_service.customer_has_business_unit
			                                                        on business_unit.id_business_unit =
			                                                           customer_has_business_unit.fk_business_unit
		                                              left join consolidated_list_of_customers on
			                                         consolidated_list_of_customers."id_customer" =
			                                         customer_has_business_unit.fk_customer
	                                         where status = 'PROCESSED'
	                                         group by id_customer
                                         )


   , last_supplier_request as (
	                              select consolidated_list_of_customers."id_customer"
			                           , max( document_upload.created_at ) "Last List Upload"
	                              from fdw_customer_service.document_upload
		                                   left join fdw_customer_service.business_unit
			                                             on business_unit.id_business_unit =
			                                                document_upload.fk_customer_business_unit
		                                   left join fdw_customer_service.customer_has_business_unit
			                                             on business_unit.id_business_unit =
			                                                customer_has_business_unit.fk_business_unit
		                                   left join consolidated_list_of_customers on
			                              consolidated_list_of_customers."id_customer" =
			                              customer_has_business_unit.fk_customer

	                              group by id_customer
                              )

-- here i am calculating the status based on the CTE before (status)
   , calculated_status as (
	                          select consolidated_list_of_customers.id_customer


			                       , "First Order"
			                       , "Last Order"
			                       , consolidated_list_of_customers."registration date" as "Registration Date"

			                       , case
				                         when ( CURRENT_DATE - order_details."Last Order"::date ) <= 27
					                         then 'Aktiver Kunde letzte 28 Tage'
				                         when ( CURRENT_DATE - order_details."First Order"::date ) <= 27
					                         then 'Neukunde letzte 28 Tage'
				                         when ( CURRENT_DATE - order_details."Last Order"::date ) >= 27
					                         then 'Inaktiver Kunde letzte 28 Tage'

				                         when ( consolidated_list_of_customers.active like '%No%'
					                         or
					                            "activated by merchant" like '%INVI%'
					                         or
					                            "registration date" is null
					                         or
					                            customer_invitation_date is not null )
					                         -- AKA customer with no parent means he has no merchant connected

					                         then 'Merchant Aktivierung austehend'
				                         else
					                         'Registrierter Kunde ohne Bestellung'
			                         end                                                as status
	                          from consolidated_list_of_customers
		                               left join order_details
			                                         on consolidated_list_of_customers.id_customer = order_details.id_customer
                          )


   , final as (
	              select consolidated_list_of_customers."Horeca Email"
			           , "Horeca Name"
			           , app
			           , "UUID"

			           , concat( address , ' ' , house_number ) as address

			           , "Zip Code"
			           , "City"
			           , "registration date"
			           , customer_invitation_date
			           , consolidated_list_of_customers.id_customer
			           , "activated by merchant"
			           , "GFGH"
			           , active
			           , calculating_reactivation_flag."Last Order"::date
			           , calculating_reactivation_flag."First Order"::date
			           , calculating_reactivation_flag."Order Status"
			           , calculating_reactivation_flag."Number Of Orders"
			           , calculating_reactivation_flag."Customer_reactivation_flag"
			           , calculating_reactivation_flag."average days between orders"
			           , calculating_reactivation_flag."average days between orders last 3 months"
			           , last_supplier_request."Last List Upload"
			           , last_successful_supplier_request."Last Successful List Upload"
			           , calculated_status.status
			           , mobile_phone
			           , calculating_reactivation_flag."Last Order last 3 months"
			           , calculating_reactivation_flag."First Order last 3 months"
			           , "Number Of Orders last 3 months"

	              from consolidated_list_of_customers
		                   left join calculating_reactivation_flag
			                             on consolidated_list_of_customers.id_customer =
			                                calculating_reactivation_flag.id_customer
		                   left join last_successful_supplier_request
			                             on last_successful_supplier_request.id_customer =
			                                consolidated_list_of_customers.id_customer
		                   left join calculated_status
			                             on calculated_status.id_customer =
			                                consolidated_list_of_customers.id_customer
		                   left join last_supplier_request
			                             on last_supplier_request.id_customer =
			                                consolidated_list_of_customers.id_customer
              )


-- now i need to consolidate these lists into one big happy table
select --distinct on (customer_has_user_and_role.id_customer_has_user_and_role)
	id_customer
	 , final."Horeca Name"  as "Customer Name"
	 , final."Horeca Email" as "Customer Email"
	 , status               as "Customer Status"
	 , "Number Of Orders"
	 , "registration date"  as "Registration Date"
	 , customer_invitation_date
	 , "Last Order"         as "Last Order Created"
	 , "UUID"
	 , "Order Status"       as "Status Last Order"
	 , "Last Successful List Upload"
	 , "Last List Upload"   as "Status Last Supplier Request"
	 , "GFGH"
	 , address              as "Street"
	 , "Zip Code"
	 , "City"
	 , "Customer_reactivation_flag"
	 , "Last Order last 3 months" ::date
	 , "First Order last 3 months" ::date
	 , "average days between orders"
	 , "average days between orders last 3 months"
	 , "Number Of Orders last 3 months"
	 , mobile_phone
from final




