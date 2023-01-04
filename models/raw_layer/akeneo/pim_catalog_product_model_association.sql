select * from akeneo_dwh.pim_catalog_product_model_association
where _sdc_extracted_at >= NOW() - '2 hour'::interval
