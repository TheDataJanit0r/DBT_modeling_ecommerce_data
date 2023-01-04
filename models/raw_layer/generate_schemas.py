import ast


def get_merchants_from_macro(macro_string):
    macro_string = merchants.split("return((")[-1].split("))")[0]
    macro_string = f"[ {macro_string} ]"
    return ast.literal_eval(macro_string)


filename = "merchants_new.sql"


with open(f"../../macros/{filename}") as f:
    merchants = f.read()

merchants_new = get_merchants_from_macro(merchants)

filename = "merchants_active.sql"


with open(f"../../macros/{filename}") as f:
    merchants = f.read()
merchants_active = get_merchants_from_macro(merchants)

merchants_all = merchants_active + merchants_new

x = []

for y in merchants_all:
    x.append((f"      - name: {y}"))

string_merchants = "\n".join(x)

schema_file = f"""version: 2

sources:
  - name: gfghdata_lambda
    schema: prod_raw_layer
    loader: copy_and_paste lambda 
    loaded_at_field: _sdc_extracted_at
    freshness:
      warn_after: {{count: 2, period: hour}}
      error_after: {{count: 12, period: hour}}
    tables:
      - name: gfgh_info
      - name: gfgh_data
        tests:
          - dbt_utils.recency:
              datepart: day
              field: _sdc_extracted_at
              interval: 180
        columns:
          - name: merchant_key
            tests: 
              - dbt_utils.at_least_one

  - name: merchant_bases
    schema: prod_raw_layer
    loader: db-extraction-gfghdata (logs on kibana)
    loaded_at_field: _sdc_extracted_at
    tables:
{string_merchants}     

models:
  - name: gs1data
  - name: gs1_matches
  - name: kxe_pim_final_skus
    tests:
      - unique:
          column_name: product_uuid
          severity: warn
  
  - name: holdings
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique
          
  
          
  - name: merchants_active
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique
  - name: merchants_new
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique
  - name: merchants_on_hold
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique
  - name: merchants_all
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique 
  - name: merchants_to_exclude
    columns:
      - name: merchant_key
        tests: 
          - not_null
          - unique

"""

with open(f"raw_selects/schema.yml", "w+") as f:
    f.write(schema_file)

#
# x = []
#
# for y in merchants_all:
#     x.append((f"  - name: {y}_enabled_with_sku"))
#     x.append((f"  - name: {y}_qs"))
#
# string_merchants = "\n".join(x)
#
#
# schema_file_qs = f"""version: 2
#
# models:
#   - name: akeneo_gfghdata_mismatch
#   - name: all_skus
#   - name: all_gtins
#   - name: all_unmatched_gtins
#   - name: csvexchange_merchants
#
# {string_merchants}
#
#   - name: qs_mix_packaging_sku
#   - name: qs_base
#   - name: qs_level_1_sku
#   - name: qs_level_1
#   - name: qs_level_2
#   - name: qs_lvl1_agg
#
#   - name: qs_base_on_id
#   - name: qs_level_1_sku_on_id
#   - name: qs_level_1_on_id
#   - name: qs_level_2_on_id
#   - name: qs_lvl1_agg_on_id
#
#   - name: skus_to_reactivate
#   - name: skus_to_deactivate
#   - name: kpis
#   - name: all_categories
#   - name: attribute_options
#   - name: missing_skus
#   - name: missing_gfgh_id
#   - name: missing_flag
#   - name: organic_certified
#   - name: nutrient_precision_missing
#   - name: pim_classifier_base
#   - name: food_classifier_base
#
#   - name: gfgh_active
#   - name: gfgh_gs1_match
#   - name: gfgh_model_match
#   - name: gfgh_new_active
#   - name: gfgh_no_gtin
#   - name: gfgh_qs
#   - name: gfgh_qs_overall
#   - name: gfgh_sku_match
#   - name: gfgh_unmatched
#   - name: gfgh_gtins
#   - name: gfgh_ongoing_overview
#   - name: gfgh_duplicate_ids
#   - name: gfgh_inconsistent_qs
#   - name: gfgh_nonexisting_ids
#   - name: gfgh_duplicate_gtin_pack
#   - name: gfgh_duplicate_gtin_single
#
#   - name: gfgh_data_no_sku
#   - name: gfgh_data_direct_releases
#   - name: gfgh_zombies
#
#   - name: new_gfgh_active
#   - name: new_gfgh_gs1_match
#   - name: new_gfgh_model_match
#   - name: new_gfgh_new_active
#   - name: new_gfgh_no_gtin
#   - name: new_gfgh_qs
#   - name: new_gfgh_qs_overall
#   - name: new_gfgh_sku_match
#   - name: new_gfgh_unmatched
#   - name: new_gfgh_ongoing_overview
#
#   - name: all_gfgh_overview
#   - name: all_gfgh_kpis
#
# """
#
# with open(f"matching/schema.yml", "w+") as f:
#     f.write(schema_file_qs)
