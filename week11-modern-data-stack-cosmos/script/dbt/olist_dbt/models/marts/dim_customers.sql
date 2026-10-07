{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM {{ ref('stg_olist_customers') }}
