
-- FILE 1: stg_online_orders.sql
-- Staging for online orders (referenced by wrk_multi_channel_sales first UNION branch)
SELECT 
    order_id,
    customer_id,
    order_date::date as order_date,
    base_amount::decimal(10,2) as base_amount,
    shipping_cost::decimal(10,2) as shipping_cost,
    online_discount::decimal(10,2) as online_discount,
    order_status,
    payment_method,
    current_timestamp() as loaded_at
FROM ph_land_db.sales.online_orders
WHERE order_date >= '2024-01-01' 
  AND order_id IS NOT NULL
  AND base_amount > 0;