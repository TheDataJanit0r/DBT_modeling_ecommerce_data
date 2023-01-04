{% macro ongoing_duplicates(GFGH_LIST) %}

{% for GFGH in GFGH_LIST %}

select
   '{{GFGH}}' AS gfgh,
   {{GFGH}}_id as id,
   MAX(identifier) as identifier
from
   prod_raw_layer.all_skus k 
where
{{GFGH}}_enabled = 'true' group by {{GFGH}}_id having count({{GFGH}}_id) > 1

{% if not loop.last %} 
union 
{% endif %}

{% endfor %}


{% endmacro %}
