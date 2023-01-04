
select     all_skus.identifier
		,  all_skus.amount_single_unit
		,  all_skus.pim_family
		,  all_skus.brand
		,  all_skus.title
		,  all_skus.net_content
		,  all_skus.net_content_uom

		, gs1s.description                                                                           AS gs1_desc

		, gs1s.short_description                                                                     AS gs1_short_desc

		, gs1s.amount                                                                                AS gs1_amount

		, gs1s.amount_text                                                                           AS gs1_amount_text

		, coalesce( gs1s.data::JSON -> '0 - Identifizierung   Klassifiz' -> 'IsTradeItemABaseUnit' ) as gs1_base_unit

		, coalesce( gs1s.data::JSON -> '0 - Identifizierung   Klassifiz' -> 'NetContent[0]' ->>
					'value' )                                                                        as gs1_net_content
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
where (all_skus.pim_family like '%Lebensmittel%' or pim_family like '%Sonstig%') 
		and 
		(shop_enabled = 'false' or shop_enabled IS NULL)  
		and 
		all_skus.status_base = 'Freigegeben' 
		and (
		false
		{% for GFGH_2 in  merchants_new() %}
      
               {% if 'test' not in GFGH_2 %}
                  or all_skus.{{GFGH_2}}_enabled  like '%rue%'
                  
               {% endif %}
      {% endfor %}
		)
		