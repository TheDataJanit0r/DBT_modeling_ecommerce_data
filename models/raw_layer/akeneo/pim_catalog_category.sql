select * from akeneo_dwh.pim_catalog_category
where _sdc_extracted_at >= NOW() - '2 hour'::interval
