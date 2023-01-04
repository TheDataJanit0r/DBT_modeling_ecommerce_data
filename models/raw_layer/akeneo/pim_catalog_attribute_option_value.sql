select * from akeneo_dwh.pim_catalog_attribute_option_value
where _sdc_extracted_at >= NOW() - '2 hour'::interval
