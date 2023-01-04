{% macro update_gfgh_import_diff_weekly() %}

-- diff bugs improvement
update prod_raw_layer.gfgh_import_diff_weekly
set name_diff = false
where replace(name_old,' ','') = replace(name_new, ' ', '');

update prod_raw_layer.gfgh_import_diff_weekly
set gtin_diff = false
where gtin_old is null and gtin_new is not null
or
gtin_old is not null and gtin_new is null
or
gtin_new is null and gtin_old is null
or
gtin_new = gtin_old;

{% endmacro %}
