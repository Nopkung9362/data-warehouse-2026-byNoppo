{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_olist_orders') }}
),
items AS (
    SELECT * FROM {{ ref('stg_olist_order_items') }}
),
customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),
products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    DATE(o.order_purchase_timestamp) AS sale_date,
    c.customer_state,
    p.category_name_english,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    SUM(i.price) AS total_revenue,
    SUM(i.freight_value) AS total_freight_cost
FROM orders o
JOIN items i ON o.order_id = i.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON i.product_id = p.product_id
GROUP BY DATE(o.order_purchase_timestamp), c.customer_state, p.category_name_english
