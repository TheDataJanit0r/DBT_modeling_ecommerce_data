with source as (
    
    select * from zendesk.organizations
    
),

renamed as (
    
    select
        
        --ids
        id as organization_id,
        
        --fields
        name,
        
        --dates
        created_at,
        updated_at
        
    from source
    
)

select * from renamed
