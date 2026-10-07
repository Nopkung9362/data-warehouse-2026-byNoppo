{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_city,
    seller_state
FROM {{ ref('stg_olist_sellers') }}
