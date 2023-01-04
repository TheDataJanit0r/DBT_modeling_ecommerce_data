
{% macro merchants_all() %}
{{ return(( merchants_active() + merchants_new() ))  }}
{% endmacro %}
