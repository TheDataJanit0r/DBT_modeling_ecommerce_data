select
  distinct l.level_1_code,
  l.type_single_unit,
  l.net_content_liter,
  l.gs1_net_content,
  l.gs1_net_content_uom,
  l.gs1_dpg,
  l.gtin_single_unit,
  l.base_title,
  b.brand,
  b.title,
  l.gs1_desc,
  l.gs1_short_desc,
  l.equal_manufacturer_glns,
  l.detail_type_single_unit,

  {% for GFGH in  merchants_all()  %}
  
  max(l.{{GFGH}}) as {{GFGH}},

  {% endfor %}

  max(l.gs1_base_unit) as gs1_base_unit
  
from
  {{ref('qs_level_1')}} l
  left join prod_raw_layer.all_skus b on l.level_1_code = b.l1_code
group by
  1,2,3,4,5,6,7,8,9,10,11,12,13,14
  
  