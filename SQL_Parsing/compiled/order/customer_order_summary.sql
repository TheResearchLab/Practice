-- FILE: customer_order_summary.sql (Mart Layer)  
-- Intermediate aggregated table

WITH customer_metrics AS (
    SELECT 
        wof.customer_id,
        wof.customer_name,
        COUNT(wof.order_id) as total_orders,
        SUM(wof.order_amount) as total_revenue,
        MIN(wof.order_date) as first_order_date,
        MAX(wof.order_date) as last_order_date,
        CASE 
            WHEN SUM(wof.order_amount) > 1000 THEN SUM(wof.order_amount) * 1.2
            ELSE SUM(wof.order_amount)
        END as customer_lifetime_value
    FROM ph_dw_prod.work.wrk_orders_final wof
    GROUP BY wof.customer_id, wof.customer_name
)
SELECT 
    cm.customer_id,
    cm.customer_name,
    cm.total_orders,
    cm.total_revenue,
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_lifetime_value,
    CASE 
        WHEN cm.last_order_date >= DATEADD(day, -90, CURRENT_DATE()) THEN 1 
        ELSE 0 
    END as is_active
FROM customer_metrics cm


