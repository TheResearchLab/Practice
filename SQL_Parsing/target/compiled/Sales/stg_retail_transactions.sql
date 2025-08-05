-- FILE 2: stg_retail_transactions.sql  
-- Staging for retail transactions (referenced by wrk_multi_channel_sales second UNION branch)
SELECT 
    transaction_id,
    cust_id,
    store_id,
    transaction_date::date as transaction_date,
    purchase_amount::decimal(10,2) as purchase_amount,
    store_discount::decimal(10,2) as store_discount,
    status,
    payment_type,
    cashier_id,
    current_timestamp() as loaded_at
FROM ph_land_db.sales.retail_transactions  
WHERE transaction_date >= '2024-01-01'
  AND transaction_id IS NOT NULL
  AND purchase_amount > 0
  AND status IN ('PAID', 'COMPLETED');