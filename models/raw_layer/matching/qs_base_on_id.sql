select
  base_code,
  status_base,

  {% for GFGH in  merchants_new()  %}
	{{GFGH}}_enabled as {{GFGH}}
	{% if not loop.last %} , {% endif %}
  {% endfor %}

from
  prod_raw_layer.all_skus
where
  base_code is not null
  and (coalesce(status_base, '') <> '6a9b3601' or status_base is null)
  and coalesce(status_base, '') <> 'Freigegeben'
