with source as (
    
    select * from zendesk.tickets
    
), 

renamed as (

    select
        --ids
        id as ticket_id,
        organization_id,
        assignee_id,
        brand_id,
        group_id,
        requester_id,
        submitter_id,
        
        --fields
        status,
        priority,
        description,
        subject,
        recipient,
        "type" as ticket_type,
        url as api_url,
        satisfaction_rating__score as satisfaction_rating_score,
        
        --dates
        created_at,
        updated_at
        
    from source
    
)

select * from renamed
