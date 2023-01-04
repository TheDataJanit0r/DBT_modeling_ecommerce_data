select
  base_code, 
  status_base 

  
   {% for GFGH in  merchants_new() %}
         {% if 'test' not in GFGH %}
               
               
                  ,all_skus.{{GFGH}}_enabled AS {{GFGH}}
            {% endif %}
         
    
   {% endfor %}
  
from 
  prod_raw_layer.all_skus
where 
  (status_base = 'QS' or status_base is null)
  and (
		false
		{% for GFGH_2 in  merchants_new() %}
      
               {% if 'test' not in GFGH_2 %}
                  or all_skus.{{GFGH_2}}_enabled  like '%rue%'
                  
               {% endif %}
      {% endfor %}
		)
  --and (family <> 'Lebensmittel' or family <> 'Sonstiges (Gastro-Artikel)')
