{{ config(materialized='view') }}

SELECT
    CAST(product_id AS VARCHAR) AS product_id,
    COALESCE(TRIM(product_category_name), 'unknown') AS product_category_name,
    CAST(product_weight_g AS INT) AS product_weight_g
FROM {{ source('raw_olist', 'olist_products') }}
