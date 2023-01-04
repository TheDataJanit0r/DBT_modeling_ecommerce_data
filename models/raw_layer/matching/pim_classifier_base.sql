/* 
 # PIM classifier
 Good inspiration: [Categorizing Products at Scale – Shopify Engineering](https://engineering.shopify.com/blogs/engineering/categorizing-products-at-scale)
 
 Motivation: Bag of words approach (https://github.com/kollex/bi-merchant-matching) not too satisfying 
 
 Constrains: Too many different articles for a new multi-class classifier. 
 
 Possible classification steps: 
 1. Use existing category classifier to classify categories
 2. Classify within a pim category the base (and liquid code?)
 3. Classify within a pim category (and within base code?) the the packaging (e.g. 24x0,33 (amount x single unit)with 4x6
 
 For example: we have around 2500 beer base codes with 70 different sizes —> should result in much better accuracy than classifying to all 25K products.
 
 */
WITH gfgh_base AS (
     select distinct  sku
                    , replace( name, '  ', '' ) as                                          name_from_merchant
                    , case when base_unit_content <> 0 then base_unit_content else NULL end base_unit_content
                    , case when no_of_base_units <> 0 then no_of_base_units else NULL end   no_of_base_units
                    , sales_unit_pkgg
    FROM
       prod_raw_layer.gfgh_data
     where sku is not null
		                and kollex_active = TRUE
		                and active = TRUE
		                and (direct_shop_release = FALSE and was_direct_release = FALSE)
        AND merchant_key NOT IN {{merchants_to_exclude()}}
        AND special_case = FALSE
    ORDER BY 1
)
SELECT
       gfgh_base.name_from_merchant
	 , gfgh_base.sku
	 , gfgh_base.base_unit_content
	 , gfgh_base.no_of_base_units
	 , gfgh_base.sales_unit_pkgg
     , base_code
	 , l1_code
	 , CONCAT( brand, ' ', title, ' ', amount_single_unit, 'x', net_content )::text as sku_title
	 , pim_category
	 , structure_packaging_unit
	 , type_packaging_unit
	 , concat( amount_single_unit, ' x ', net_content )                             as size
	 , type_single_unit
FROM
    gfgh_base
    LEFT JOIN prod_raw_layer.all_skus ON all_skus.sku = identifier
WHERE
    enablement > 0
    AND type_packaging_unit <> 'FREAK (abweichendes Design, Rücksprache halten)'