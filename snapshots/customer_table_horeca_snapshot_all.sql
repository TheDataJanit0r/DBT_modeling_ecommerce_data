{% snapshot customer_table_horeca_snapshot_all %}

    {{
        config(
          target_schema='snapshots',
           strategy='check',
          unique_key='id_customer',
         check_cols='all'
        )
    }}

    select id_customer,"Customer Name","Customer_reactivation_flag","Customer Email","Customer Status" 
    from {{ ref('customer_table_horeca_children_customers') }}

{% endsnapshot %}