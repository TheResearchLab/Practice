-- Compiled fct_orders model (built from snapshot)
with orders_current as (
    select 
        order_id,
        customer_id,
        order_date,
        order_amount,
        order_channel,
        product_category,
        quantity,
        discount_amount,
        tax_amount,
        shipping_cost,
        order_status,
        store_location,
        mobile_device_type,
        app_version,
        dbt_valid_from,
        dbt_valid_to,
        updated_at
    from ph_idea_play.aar58466.snp_orders
    where dbt_valid_to is null  -- Current records only
)

select 
    order_id,
    customer_id,
    order_date,
    order_amount,
    order_channel,
    product_category,
    quantity,
    discount_amount,
    tax_amount,
    shipping_cost,
    (order_amount - discount_amount + tax_amount + coalesce(shipping_cost, 0)) as total_amount,
    order_status,
    store_location,
    mobile_device_type,
    app_version,
    updated_at
from orders_current