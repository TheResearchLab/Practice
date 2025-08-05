-- FILE 1: wrk_multi_channel_sales.sql
-- Simple UNION ALL combining different sales channels
WITH channel_sales AS (
    SELECT 
        order_id,
        customer_id,
        'ONLINE' as channel,
        online_discount as discount_amount,
        base_amount + shipping_cost as total_amount
    FROM ph_land_db.sales.online_orders
    WHERE order_status = 'COMPLETED'
    
    UNION ALL
    
    SELECT 
        transaction_id as order_id,
        cust_id as customer_id, 
        'RETAIL' as channel,
        store_discount as discount_amount,
        purchase_amount as total_amount
    FROM ph_land_db.sales.retail_transactions
    WHERE status = 'PAID'
    
    UNION ALL
    
    SELECT 
        mobile_order_id as order_id,
        user_id as customer_id,
        'MOBILE' as channel, 
        app_discount as discount_amount,
        order_total as total_amount
    FROM ph_land_db.sales.mobile_purchases
    WHERE payment_status = 'SUCCESS'
)

SELECT 
    order_id,
    customer_id,
    channel,
    discount_amount,
    total_amount,
    current_timestamp() as processed_at
FROM channel_sales