-- Compiled stg_orders_retail model (staging layer)
select
    order_id,
    customer_id, 
    order_date::date as order_date,
    order_amount,
    'RETAIL' as order_channel,
    product_category,
    quantity,
    discount_amount,
    tax_amount,
    0 as shipping_cost, -- No shipping for retail
    order_status,
    store_location,
    current_timestamp() as updated_at
from raw_data_db.order_data.orders_retail  
where order_id is not null
    and order_amount > 0