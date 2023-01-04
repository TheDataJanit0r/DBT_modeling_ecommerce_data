WITH codes AS (
    SELECT
        "Brick_Code" brick_id,
        "Brick_Description" brick_desc,
        "Family_Code" AS family_id,
        "Family_Description" AS family_desc,
        "Class_Code" class_id,
        "Class_Description" class_desc
    FROM
        sheet_loader.gs1_brick_classification
),
main AS (
    SELECT
        *,
        data :: json -> '0 - Identifizierung   Klassifiz' ->> 'GPCCategoryCode' brick_id
    FROM
        {{ref('gs1data')}}
),
final AS (
    SELECT
        liquid_code,
        description,
        short_description,
        gtin_packaging_unit,
        gtin_single_unit,
        brand,
        c.*
    FROM
        main m
        LEFT JOIN codes c USING (brick_id)
)
SELECT
    *
FROM
    final
WHERE
    brick_id IS NOT NULL