with timestamps_qr_forms  as (


            select distinct on (customer_table_horeca_jvp_classifed.id_customer) cth_classified.id_customer,
                                                                                  coalesce(
                                                                                          coke_matched_stammdaten.timestamp,
                                                                                          rotkaepchen_matched_stammdaten.timestamp,
                                                                                          bitburger_matched_stammdaten.timestamp,
                                                                                          krombacher_matched_stammdaten.timestamp) JVP_timestamp
             from {{ref('customer_table_horeca_children_customers_classified')}}  cth_classified
                      left join fdw_customer_service.customer_has_parent
                                on customer_has_parent.fk_customer = cth_classified.id_customer
                      left join prod_info_layer.customer_table_horeca_jvp_classifed
                                on customer_has_parent.fk_customer_parent =
                                   customer_table_horeca_jvp_classifed.id_customer
                      left join prod_info_layer.coke_matched_stammdaten
                                on cth_classified.id_customer = coke_matched_stammdaten."Kollex Customer ID"
                      left join prod_info_layer.krombacher_matched_stammdaten
                                on cth_classified.id_customer = krombacher_matched_stammdaten.id_customer
                      left join prod_info_layer.rotkaepchen_matched_stammdaten
                                on cth_classified.id_customer = rotkaepchen_matched_stammdaten."Kollex Customer ID"
                      left join prod_info_layer.bitburger_matched_stammdaten
                                on cth_classified.id_customer = bitburger_matched_stammdaten.id_customer
             )


select distinct on (cth_classified.id_customer) cth_classified.id_customer                                 as "Parent ID",
                                                'HORECA'                                                   as Typ,
                                                cth_classified."Customer Name"                             as "Unternehmensname",
                                                cth_classified."Customer Email"                            as "E-Mail Adresse Owner",
                                                cth_classified."Customer Status",
                                                cth_classified."Number Of Orders",
                                                cth_classified."Registration Date"                         as "Wann registriert",
                                                cth_classified."Last Order Created"                        as "Wann zuletzt bestellt",

                                                cth_classified."Status Last Order",
                                                cth_classified."Last Successful List Upload"               as "Wann letzter Upload",
                                                cth_classified."Status Last Supplier Request",
                                                cth_classified."Street"                                    as "Straße",
                                                cth_classified."Zip Code"                                  as "PLZ",
                                                cth_classified."City"                                      as "Ort",

                                                case
                                                    when cth_classified."Customer Status" like '%Aktiv%'
                                                        then true
                                                    else false end                                         as "Gilt als aktiv",
                                                case
                                                    when cth_classified."Customer Status" like '%Registrierter%'
                                                        then true
                                                    else false end                                         as "Registrierter Kunde ohne Bestellung",
                                                case
                                                    when cth_classified."Customer Status" like '%Merchant%'
                                                        then true
                                                    else false end                                         as "Merchant Aktivierung offen",
                                                case
                                                    when cth_classified."Customer Status" like '%Inaktiv%'
                                                        then true
                                                    else false end                                         as "Gilt als inaktiv (hat bereits bestellt, ist seit 28 inaktiv)",
                                                cth_classified."average days between orders"               as "Durchschnittlicher Bestellrythmus (allgemein)",
                                                cth_classified."average days between orders last 3 months" as "Wie oft durchschnittlichen in den letzten 3 Monaten bestellt",
                                                cth_classified."Customer_reactivation_flag",
                                                --cth_classified.number_of_added_merchants                   as "Wie viele Lieferanten angelegt",

                                                contact.email,
                                                contact.last_name                                          as "Name Owner",
                                                contact.first_name                                         as "Vorname Owner",

                                                contact.mobile_phone                                       as "Telefonnummer Owner",
                                                cth_classified."Coke Kunde",
                                                cth_classified."Bitburger Kunde",
                                                cth_classified."Krombacher Kunde",
                                                cth_classified."Rottkapechen Kunde",
                                                JVP_timestamp                                              as "Wann JV-Typeform ausgefüllt",
                                                cth_classified."customer_invitation_date"                  as "Wann eingeladen"
                                                


from {{ref('customer_table_horeca_children_customers_classified')}}  cth_classified
         left join fdw_customer_service.customer_has_contacts
                   on id_customer = fk_customer
         left join fdw_customer_service.contact on fk_contact = contact.id_contact
         left join fdw_customer_service.customer_has_user_and_role
                   on id_customer = customer_has_user_and_role.fk_customer
         left join timestamps_qr_forms
                   on cth_classified.id_customer = timestamps_qr_forms.id_customer

where (fk_role <> 2 or fk_role is null)