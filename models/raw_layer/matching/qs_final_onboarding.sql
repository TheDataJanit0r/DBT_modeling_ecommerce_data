select * from (
    select
        all_skus.identifier as sku,
        all_skus.structure_packaging_unit,
        all_skus.name,
        all_skus.base_unit_content,
        all_skus.no_of_base_units,
        all_skus.brand,
        all_skus.title,
        all_skus.type_packaging_unit,
        all_skus.net_content,
        all_skus.amount_single_unit,
        all_skus.pim_family,
        -- a.gtin_packaging_unit,
        --a.gtin_single_unit,
        -- b.gtin_packaging_unit,
        -- b.gtin_single_unit,
        gs1s.description as gs1_desc,
        row_number() over (partition by all_skus.identifier) as rn,
        gs1s.short_description,
        coalesce(gs1s.data::JSON -> '0 - Identifizierung   Klassifiz' -> 'NetContent[0]' ->> 'value' ) as gs1_short_desc
        {% for GFGH in  merchants_new() %}
            {% if 'test' not in GFGH %}
				, all_skus.{{GFGH}}_enabled AS {{GFGH}}
			{% endif %}
       {% endfor %}

    from prod_raw_layer.all_skus
        left join from_pim.cp_gs1data gs1s
            on gs1s.gtin_single_unit::text = all_skus.gtin::text
            or gs1s.gtin_packaging_unit::text = all_skus.gtin_packaging_unit::text
            or gs1s.gtin_single_unit = all_skus.gtin_single_unit

    where
        all_skus.release_l1 like '%rue%'
        and (
            {% for GFGH_1 in  merchants_new() %}
                {% if 'test' not in GFGH_1 %}
                    all_skus.{{GFGH_1}}_freigabe like '%alse%'
                    {% if not loop.last %}
                        or
                    {% endif %}
                {% endif %}
            {% endfor %}
            or
            {% for GFGH_2 in  merchants_new() %}
                {% if 'test' not in GFGH_2 %}
                    all_skus.{{GFGH_2}}_freigabe like '%one%'
                    {% if not loop.last %}
                        or
                    {% endif %}
                {% endif %}
            {% endfor %}
            or
            {% for GFGH_3 in  merchants_new() %}
                {% if 'test' not in GFGH_3 %}
                    all_skus.{{GFGH_3}}_freigabe is null
                    {% if not loop.last %}
                        or
                    {% endif %}
                {% endif %}
            {% endfor %}
            or
            {% for GFGH_4 in  merchants_new() %}
                {% if 'test' not in GFGH_4 %}
                    all_skus.{{GFGH_4}}_enabled like '%rue%'
                    {% if not loop.last %}
                        or
                    {% endif %}
                {% endif %}
            {% endfor %}
        )
    ) as t1
where t1.rn = 1
