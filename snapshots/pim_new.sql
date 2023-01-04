
{% snapshot pim_new %}

    {{
        config(
          target_database='dwh',
          target_schema='snapshots',
          unique_key='gfgh',
          
          strategy='check',
          check_cols=['sku_match', 'model_match', 'gs1_match', 'unmatched', 'no_gtin', 'no_sku', 'qs', 'diff', 'excluded'],
        )
    }}
    
    select * from {{ ref('new_gfgh_ongoing_overview') }}
    
{% endsnapshot %}
