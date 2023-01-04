with base as (
	             SELECT pcp.identifier AS sku
		              , fam.label      AS pim_family
		              , CASE
			                WHEN (cct_parent.label = 'Katalog')
				                then cct.label
			                WHEN (cct.label is null and cct_parent.label is null)
				                then fam.label
			                WHEN (cct.label is null and cct_parent.label is not null and cct_parent.label <> 'Katalog')
				                then cct_parent.label
			                ELSE cct_parent.label
		                end            as pim_category
		              , cct.label      AS pim_subcategory
		              , CASE
			                WHEN (cct.label is null and cct_parent.label is null)
				                then fam.label
			                WHEN (cct.label is null and cct_parent.label is not null)
				                then cct_parent.label
			                ELSE cct.label
		                end            as lowest_pim_category

	             FROM from_pim.cp_pim_catalog_product pcp

		                  LEFT JOIN from_pim.cp_pim_catalog_product_model cpm
		                            ON pcp.product_model_id = cpm.id--fam

		                  LEFT JOIN from_pim.cp_pim_catalog_product_model ccpm
		                            ON cpm.id = ccpm.id
		                  LEFT JOIN akeneo_dwh.pim_catalog_family_translation fam
		                            ON pcp.family_id = fam.foreign_key
		                  left join akeneo_dwh.pim_catalog_category_product
		                            on pcp.id = pim_catalog_category_product.product_id
		                  LEFT JOIN akeneo_dwh.pim_catalog_category cc
		                            ON cc.id = pim_catalog_category_product.category_id
		                  LEFT JOIN akeneo_dwh.pim_catalog_category_translation cct
		                            ON cc.id = cct.foreign_key
			                            AND cct.locale = 'de_DE'
--parent
		                  LEFT JOIN akeneo_dwh.pim_catalog_category cc_parent
		                            ON cc.parent_id = cc_parent.id
		                  LEFT JOIN akeneo_dwh.pim_catalog_category_translation cct_parent
		                            ON cc_parent.id = cct_parent.foreign_key
			                            AND cct_parent.locale = 'de_DE'

	             WHERE pcp.identifier IS NOT NULL
             )

--manual overriding as they seem to be weirdly mappend within akeneo structures
   , final as (
	              select sku
			           , pim_family
			           , (case
				              when pim_category in ('Dunkle Biere', 'Weitere', 'Pils',
				                                    'Dunkles Bier', 'Weitere Biere', 'Ale', 'Starkbier')
					              then 'Bier'
				              when pim_category in ('Mate- & Teegetränke')
					              then 'Softdrinks'
				              when pim_category in ('Prickelndes')
					              then 'Wein & Prickelndes'
				              when pim_category in ('Sonstiges (Gastro-Artikel)')
					              then 'Gastro-Bedarf'
				              when pim_category in ('Mixpaletten, -displays, -kartons')
					              then 'Mehr'
				              else pim_category
			              end) as pim_category
			           , pim_subcategory

			           , lowest_pim_category

	              from base
              )

select sku
	 , pim_family
	 , pim_category
	 , max( pim_subcategory )     as pim_subcategory
	 , max( lowest_pim_category ) as lowest_pim_category
from final
group by 1, 2, 3
