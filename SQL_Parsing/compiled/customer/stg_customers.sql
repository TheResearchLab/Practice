-- FILE: stg_customers.sql (Staging Layer)
-- Customer data cleaning

SELECT 
    customer_id,
    TRIM(CONCAT(first_name, ' ', last_name)) as customer_name,
    LOWER(TRIM(email)) as email,
    signup_date::DATE as signup_date,
    CASE 
        WHEN customer_status = 'A' THEN 'active'
        WHEN customer_status = 'I' THEN 'inactive'
        ELSE 'unknown'
    END as status
FROM raw_crm_system.customers.customer_master
WHERE customer_id IS NOT NULL