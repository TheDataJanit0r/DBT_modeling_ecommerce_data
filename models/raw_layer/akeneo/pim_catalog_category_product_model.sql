select * from akeneo_dwh.pim_catalog_category_product_model
where _sdc_extracted_at >= NOW() - '2 hour'::interval
