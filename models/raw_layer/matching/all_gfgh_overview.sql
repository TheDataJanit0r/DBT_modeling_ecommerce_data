{% for GFGH in  merchants_all() %}

select
    '{{ GFGH }}' as gfgh,
    'skus_by_family' as focus,
    pim_family as key,
    count(identifier) as value
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'
group by 1, 2, 3

union
--skus in akeneo
select
    '{{ GFGH }}' as gfgh,
    'skus_in_akeneo' as focus,
    'current',
    count(sku)
from {{ref(GFGH)}} as a
where direct_shop_release = 'false' and a.kollex_active = true

union
select
    '{{ GFGH }}' as gfgh,
    'skus_in_akeneo' as focus,
    'previous',
    count(id)
from {{ref(GFGH)}} b
where direct_shop_release = 'false' and b.kollex_active = true

union
select
    '{{ GFGH }}' as gfgh,
    'articles_with_gtin' as focus,
    'true' as key,
    count(identifier)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%' and is_manual = 0

union
select
    '{{ GFGH }}' as gfgh,
    'articles_with_gtin' as focus,
    'false' as key,
    count(identifier)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%' and is_manual = 1

union
--pictures
select
    '{{ GFGH }}' as gfgh,
    'articles_picture' as focus,
    'true' as key,
    count(p.sku)
from prod_raw_layer.all_skus s
join {{ref('sku_pictures')}} p on p.sku = s.identifier
where {{ GFGH }}_enabled like '%rue%'

union
--qs steps
select
    '{{ GFGH }}' as gfgh,
    'qs_base' as focus,
    'current',
    count(distinct base_code)
from prod_raw_layer.all_skus
where lower(status_base) = 'freigegeben' and {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_base' as focus,
    'previous',
    count(distinct base_code)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_level1' as focus,
    'current',
    count(distinct l1_code)
from prod_raw_layer.all_skus
where release_l1 like '%rue%' and {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_level1' as focus,
    'previous',
    count(distinct l1_code)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_shopfreigabe' as focus,
    'current',
    count(distinct identifier)
from prod_raw_layer.all_skus
where shop_enabled like '%rue%' and {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_shopfreigabe' as focus,
    'previous',
    count(distinct identifier)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_gfghfreigabe' as focus,
    'current',
    count(distinct identifier)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'

union
select
    '{{ GFGH }}' as gfgh,
    'qs_gfghfreigabe' as focus,
    'previous',
    count(distinct identifier)
from prod_raw_layer.all_skus
where {{ GFGH }}_enabled like '%rue%'

{% if not loop.last %} union {% endif %}
{% endfor %}
