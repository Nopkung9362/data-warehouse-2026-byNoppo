{{ config(materialized='view') }}

SELECT
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(customer_unique_id AS VARCHAR) AS customer_unique_id,
    CAST(customer_city AS VARCHAR) AS customer_city,
    CAST(customer_state AS VARCHAR) AS customer_state
FROM {{ source('raw_olist', 'olist_customers') }}
