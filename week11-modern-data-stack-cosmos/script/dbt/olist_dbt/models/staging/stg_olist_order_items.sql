{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_item_id AS INT) AS order_item_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(price AS NUMERIC(10,2)) AS price,
    CAST(freight_value AS NUMERIC(10,2)) AS freight_value
FROM {{ source('raw_olist', 'olist_order_items') }}
