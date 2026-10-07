{{ config(materialized='view') }}

SELECT
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(seller_city AS VARCHAR) AS seller_city,
    CAST(seller_state AS VARCHAR) AS seller_state
FROM {{ source('raw_olist', 'olist_sellers') }}
