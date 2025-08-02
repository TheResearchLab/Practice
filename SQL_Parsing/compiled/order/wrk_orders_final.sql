-- FILE: wrk_orders_final.sql (Work Layer)
-- Business logic and joins

SELECT 
    o.order_id,
    c.customer_id,
    c.customer_name,
    o.order_date,
    o.order_amount,
    p.product_category,
    CASE 
        WHEN o.order_amount > 500 THEN 'high_value'
        WHEN o.order_amount > 100 THEN 'medium_value'
        ELSE 'low_value'
    END as order_tier
FROM ph_dw_prod.staging.stg_orders o
INNER JOIN ph_dw_prod.staging.stg_customers c 
    ON o.customer_id = c.customer_id
LEFT JOIN ph_dw_prod.staging.stg_products p 
    ON o.product_id = p.product_id
WHERE o.order_status = 'completed'


