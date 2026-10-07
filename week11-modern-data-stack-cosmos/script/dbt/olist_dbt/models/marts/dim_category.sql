{{ config(materialized='table') }}

SELECT
    MD5(category_name_portuguese) AS category_key,
    category_name_portuguese,
    category_name_english
FROM {{ ref('stg_product_category_translation') }}
