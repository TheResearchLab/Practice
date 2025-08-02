-- FILE: fct_order_details.sql (Another Presentation Layer Table)
-- Detailed order analysis table
-- Test with: table="fct_order_details", column="order_value_tier"

SELECT 
    wof.order_id,
    wof.customer_id,
    wof.customer_name,
    wof.order_date,
    wof.order_amount,
    wof.product_category,
    wof.order_tier,
    CASE 
        WHEN wof.product_category = 'ELECTRONICS' AND wof.order_tier = 'high_value' THEN 'premium_tech'
        WHEN wof.product_category = 'ELECTRONICS' THEN 'standard_tech'
        WHEN wof.order_tier = 'high_value' THEN 'premium_other'
        ELSE 'standard'
    END as order_value_tier,
    CASE 
        WHEN wof.order_date >= DATEADD(day, -30, CURRENT_DATE()) THEN 'recent'
        WHEN wof.order_date >= DATEADD(day, -90, CURRENT_DATE()) THEN 'moderate'
        ELSE 'old'
    END as order_recency
FROM ph_dw_prod.work.wrk_orders_final wof
WHERE wof.order_tier IS NOT NULL