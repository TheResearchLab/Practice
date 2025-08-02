-- FILE: fct_customer_orders.sql (Presentation Layer)
-- This represents your final business table
-- Test with: table="fct_customer_orders", column="customer_id"

SELECT 
    co.customer_id,
    co.customer_name,
    co.total_orders,
    co.total_revenue,
    co.first_order_date,
    co.last_order_date,
    co.customer_lifetime_value
FROM ph_dw_prod.marts.customer_order_summary co
WHERE co.is_active = 1

