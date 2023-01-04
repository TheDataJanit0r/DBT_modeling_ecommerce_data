select * from akeneo_dwh.pim_catalog_category_translation
where _sdc_extracted_at >= NOW() - '2 hour'::interval
