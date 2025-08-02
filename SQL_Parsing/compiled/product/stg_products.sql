-- FILE: stg_products.sql (Staging Layer)
-- Product data cleaning

SELECT 
    product_id,
    TRIM(product_name) as product_name,
    UPPER(TRIM(category)) as product_category,
    price,
    created_date
FROM external_catalog_db.products.product_info
WHERE is_active = true