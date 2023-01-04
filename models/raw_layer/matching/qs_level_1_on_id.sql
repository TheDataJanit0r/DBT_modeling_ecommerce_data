
with count_duplicates as (
    select
        all_skus.l1_code,
        all_skus.type_single_unit,
        row_number() over (partition by l1_code) as rn,
        all_skus.detail_type_single_unit,
        all_skus.net_content_liter,
        coalesce( gs1s.data::JSON -> '0 - Identifizierung   Klassifiz' -> 'NetContent[0]' ->>'value' ) as gs1_net_content,
        all_skus.brand,
        all_skus.title,
        gs1s.description as gs1_desc,
        gs1s.short_description as gs1_short_desc

        {% for GFGH in  merchants_new() %}
            {% if 'test' not in GFGH %}
                ,all_skus.{{GFGH}}_enabled AS {{GFGH}}
            {% endif %}
        {% endfor %}

    from prod_raw_layer.all_skus
	left join from_pim.cp_gs1data gs1s
        on gs1s.gtin_single_unit::text = all_skus.gtin::text
        or gs1s.gtin_packaging_unit::text = all_skus.gtin_packaging_unit::text
        or gs1s.gtin_single_unit = all_skus.gtin_single_unit
    where
        (all_skus.pim_family not like '%Lebensmittel%' and  pim_family not like '%Sonstig%') and
		(all_skus.release_l1 IS NULL OR all_skus.release_l1 like '%alse%') and
		all_skus.status_base = 'Freigegeben' and
        (false
		    {% for GFGH_2 in  merchants_new() %}
                {% if 'test' not in GFGH_2 %}
                    or all_skus.{{GFGH_2}}_enabled  like '%rue%'
                {% endif %}
            {% endfor %}
		)
),

rm_duplicates as (
    select * from count_duplicates where rn = 1
)

select
    l1_code,
    type_single_unit,
    detail_type_single_unit,
    net_content_liter,
    gs1_net_content,
    brand,
    title,
    gs1_short_desc
    gs1_desc
    {% for GFGH in  merchants_new() %}
        {% if 'test' not in GFGH %}
            ,{{GFGH}}
        {% endif %}
    {% endfor %}
from rm_duplicates
