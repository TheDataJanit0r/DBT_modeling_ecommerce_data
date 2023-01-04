select
      identifier,
      status_base,
      release_l1,
      base_code,
      l1_code,
      shop_enabled,
    {% for GFGH in  merchants_all() %}
    {{GFGH}}_enabled {% if not loop.last %}, {% endif %} 
    {% endfor %}
    from
     prod_raw_layer.all_skus
    where
      (
        {% for GFGH in  merchants_all() %}
        {{GFGH}}_enabled = 'true' {% if not loop.last %} or {% endif %} 
        {% endfor %}
        
      )
      and (
        (shop_enabled = 'false')
        or (
          base_code is not null
          and coalesce(status_base, '') <> 'Freigegeben'
        )
        or (
          l1_code is not null
          and release_l1 = 'false'
        )
      )
    union
    select
      identifier,
      status_base,
      release_l1,
      base_code,
      l1_code,
      shop_enabled,
      {% for GFGH in  merchants_all() %}
    {{GFGH}}_enabled {% if not loop.last %}, {% endif %} 
    {% endfor %}
    from
      prod_raw_layer.all_skus
    where
      shop_enabled = 'true'
      and (
        (
          base_code is not null
          and coalesce(status_base, '') <> 'Freigegeben'
        )
        or (
          l1_code is not null
          and release_l1 = 'false'
        )
      )
    union
    select
      l1_code,
      status_base,
      release_l1,
      base_code,
      l1_code,
      null,
      {% for GFGH in  merchants_all() %}
    {{GFGH}}_enabled {% if not loop.last %}, {% endif %} 
    {% endfor %}
    from
      prod_raw_layer.all_skus
    where
      release_l1 = 'true'
      and base_code is not null
      and coalesce(status_base, '') <> 'Freigegeben'
  