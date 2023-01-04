select

{% for GFGH in  merchants_new()  %}

 (with direct_shop_release_{{GFGH}} as (select p.merchant_key, count(distinct gfgh_product_id) as direct_shop_releases
      from {{ref('gfgh_data')}} p 
      where  direct_shop_release = TRUE and active = TRUE and kollex_active = TRUE 
       and p.merchant_key = '{{GFGH}}' group by 1 ),

kpi_measure_{{GFGH}} as (select '{{GFGH}}' as gfgh, count(distinct sku) as kpi_count from {{ref(GFGH)}} where kollex_active = true),
kpi_measure_final_{{GFGH}} as (select kpi_count + COALESCE(direct_shop_releases, 0) from kpi_measure_{{GFGH}} 
                              left join direct_shop_release_{{GFGH}} on merchant_key = gfgh)

select * from  kpi_measure_final_{{GFGH}}) as {{GFGH}}_nom,

(with direct_shop_release_{{GFGH}} as (select p.merchant_key, count(*) as direct_shop_releases
            from {{ref('gfgh_data')}} p  
            where direct_shop_release = TRUE and active = TRUE and kollex_active = TRUE 
            and p.merchant_key = '{{GFGH}}' group by 1 ),
 
 kpi_target_{{GFGH}} as (select '{{GFGH}}' as gfgh ,count(0) as kpi_count from {{ ref('pim_catalog_product') }} p
      where p.raw_values::JSON->'gfgh_{{GFGH}}_enabled'->'<all_channels>'->>'<all_locales>' = 'true'),


kpi_target_final_{{GFGH}} as (select kpi_count + COALESCE(direct_shop_releases, 0) from kpi_target_{{GFGH}} left join direct_shop_release_{{GFGH}} on merchant_key = gfgh)

select * from  kpi_target_final_{{GFGH}}) AS {{GFGH}},




{% endfor %}
      
      
      (
   select count(0) from {{ ref('pim_catalog_product')  }} p
   where         
   p.raw_values::JSON->'shop_enabled'->'<all_channels>'->>'<all_locales>' = 'true') AS shop_enabled,
      (select count(0) from {{ ref('pim_catalog_product')  }} )AS sku,
      (
         select
            count(0) 
         from
            {{ref('pim_catalog_product_model')}} p
         where p.parent_id is null
      )
      AS liquid,
      (
         select
            count(0) 
         from
            {{ ref('pim_catalog_product')  }} p
         where p.raw_values::JSON->'gfgh_trinkkontor_enabled'->'<all_channels>'->>'<all_locales>' = 'true' and 
               p.raw_values::JSON->'gfgh_gvs_enabled'->'<all_channels>'->>'<all_locales>' = 'true'
      )
      AS tk_gvs,
      (
         select
            count(0) 
         from
            {{ ref('pim_catalog_product')  }} p
         where p.raw_values::JSON->'gfgh_trinkkontor_enabled'->'<all_channels>'->>'<all_locales>' = 'true' and 
            p.raw_values::JSON->'gfgh_krajewski_enabled'->'<all_channels>'->>'<all_locales>' = 'true'
      )
      AS tk_kj,
      (
         select
            count(0) 
         from
             {{ ref('pim_catalog_product')  }} p 
         join
             {{ref('pim_catalog_product_model')}} pm on pm.id = p.product_model_id 
         left join {{ref('pim_catalog_product_model')}} pm2 on pm2.id = pm.parent_id
            
         where
            (
            (pm2.code like 'm-%') 
               or 
               (
                  pm.parent_id is null 
                  and pm.code like 'm-%'  
               )
            )
      )
      AS manual,
      (
         select
            count(0) 
         from
            (
               ({{ ref('pim_catalog_product')  }} p
               join {{ref('pim_catalog_product_model')}} pm on((pm.id = p.product_model_id))) 
               left join {{ref('pim_catalog_product_model')}} pm2 on((pm2.id = pm.parent_id))
            )
         where
            (
            ((pm.parent_id is not null) 
               and not((pm2.code like 'm-%'))              
            ) 
               or 
            (
               pm.parent_id is null 
               and not((pm.code like 'm-%'))
            )
            )
      )
      AS imported,
      (
         select
            count(0) 
         from
            {{ ref('pim_catalog_product')  }} p 
         where
            p.product_model_id is null
      )
      AS flat,
      now() as updated_at
