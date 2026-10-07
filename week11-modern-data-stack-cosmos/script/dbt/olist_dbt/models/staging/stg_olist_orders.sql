{{ config(materialized='view') }}

SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp
FROM {{ source('raw_olist', 'olist_orders') }}
