-- depends_on: {{ ref('merchants_active') }}

{% macro merchants_active_test() %}

     {%- call statement('merchants_active', fetch_result=True) %}

        SELECT merchant_key
        FROM {{ref('merchants_active')}}

    {%- endcall -%}

    {%- set value_list = load_result('merchants_active') -%}
    
    {{ return(value_list['data']) }}


{% endmacro %}
