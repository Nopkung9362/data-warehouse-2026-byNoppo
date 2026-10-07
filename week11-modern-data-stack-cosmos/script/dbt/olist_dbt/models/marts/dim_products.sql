{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_olist_products') }}
),
categories AS (
    SELECT * FROM {{ ref('dim_category') }}
)

SELECT
    p.product_id,
    c.category_key, -- Foreign Key อ้างอิงไปยัง dim_category (Snowflake Structure)
    c.category_name_english,
    p.product_weight_g
FROM products p
LEFT JOIN categories c ON p.product_category_name = c.category_name_portuguese
