-- FILE: fct_customer_orders.sql (Presentation Layer)
-- This represents your final business table
-- Test with: table="fct_customer_orders", column="customer_id" or "customer_segment"

SELECT 
    co.customer_id,
    co.customer_name,
    co.total_orders,
    co.total_revenue,
    co.first_order_date,
    co.last_order_date,
    co.customer_lifetime_value,
    co.primary_product_category,
    co.avg_order_value,
    CASE 
        WHEN co.customer_lifetime_value > 2000 AND co.total_orders > 10 THEN 'VIP'
        WHEN co.customer_lifetime_value > 1000 THEN 'Premium'
        WHEN co.total_orders > 5 THEN 'Frequent'
        ELSE 'Standard'
    END as customer_segment
FROM ph_dw_prod.marts.customer_order_summary co
WHERE co.is_active = 1
