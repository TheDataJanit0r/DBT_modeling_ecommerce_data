with 

    {% for GFGH in merchants_all() %}
    {% if loop.first %}
    tasch_{{GFGH}} as (
    {% else %}
    ,tasch_{{GFGH}} as (
    {% endif %}
    select    sku as identifier
            , gfgh_product_id
            , {{GFGH}}.name
            , case 
                when status = 'enabled' 
                then TRUE
                else FALSE 
             end as gfgh_active
        , {{GFGH}}.kollex_active
        , direct_shop_release 
        from {{ ref (GFGH) }} 
        where sku is not null
        )
        
    ,gdata_{{GFGH}} as (
        select gfgh_product_id 
             , sku as gdata__sku
             , active as gdata__active
             , qa as gdata__qa 
        from {{ref ('gfgh_data')}}
        where merchant_key = '{{GFGH}}' 
        )

    ,agg_{{GFGH}} as (
        select 
          identifier
        , case
             when {{GFGH}}_id =  gdata_{{GFGH}}.gfgh_product_id 
             then TRUE 
             else FALSE 
        end all_good
        , {{GFGH}}_id as akeneo_id
        ,  gdata_{{GFGH}}.gfgh_product_id
        , {{GFGH}}_enabled as freigegeben
        ,  tasch_{{GFGH}}.name
        , gfgh_active
        ,  tasch_{{GFGH}}.kollex_active
        , tasch_{{GFGH}}.direct_shop_release
        , gdata__sku
        , gdata__active
        , gdata__qa
        , case 
            when gdata__sku IS NOT NULL 
            then TRUE 
            else FALSE 
        end gdata_mapping_all_good
        , case 

            when gdata__qa::BOOLEAN !=  (case when all_skus.{{GFGH}}_enabled::text = 'None' then false else true end)::BOOLEAN 
            then FALSE 
            else TRUE 
        end gdata_qa_all_good
        from prod_raw_layer.all_skus
        join tasch_{{GFGH}} using (identifier)
        join gdata_{{GFGH}} on gdata_{{GFGH}}.gfgh_product_id =  tasch_{{GFGH}}.gfgh_product_id
    )

    ,final_{{GFGH}} as (
                        select '{{GFGH}}' as merchant_key
                                , * 
                        from agg_{{GFGH}} 
                        where all_good = FALSE or gdata_mapping_all_good = FALSE or gdata_qa_all_good = FALSE
        )

    {% endfor %}
    ,final as (
    {% for GFGH in merchants_all() %}
        select * from final_{{GFGH}}
    {% if not loop.last %}
        union
    {% endif %}
    {% endfor %}
    )

select * from final
