{% for GFGH in  merchants_all() %}

select
    gpu as gtin_packaging_unit
from
    {{ ref(GFGH) }}
where
    gpu is not null
    and status = 'enabled'
    and kollex_active = TRUE 

{% if not loop.last %} union {% endif %}   
{% endfor %}
