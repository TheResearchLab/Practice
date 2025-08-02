-- FILE: stg_orders.sql (Staging Layer)
-- Data cleaning and standardization

SELECT 
    order_id,
    customer_id,
    product_id,
    order_date::DATE as order_date,
    COALESCE(order_amount, 0) as order_amount,
    UPPER(TRIM(order_status)) as order_status,
    created_at
FROM raw_ecommerce_db.public.orders
WHERE order_date >= '2020-01-01'
    AND order_id IS NOT NULL


