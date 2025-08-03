-- Compiled wrk_orders_mobile model (work layer)
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
    null as store_location,
    mobile_device_type,
    app_version,
    case 
        when order_amount >= 500 then 'HIGH_VALUE'
        when order_amount >= 100 then 'MEDIUM_VALUE'
        else 'LOW_VALUE'
    end as order_value_tier,
    updated_at
from ph_idea_play.aar58466.stg_orders_mobile
where order_status != 'CANCELLED'