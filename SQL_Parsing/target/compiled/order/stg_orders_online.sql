-- Compiled stg_orders_online model (staging layer)
select
    order_id,
    customer_id,
    order_date::date as order_date,
    order_amount,
    'ONLINE' as order_channel,
    product_category,
    quantity,
    discount_amount,
    tax_amount,
    shipping_cost,
    order_status,
    current_timestamp() as updated_at
from raw_data_db.order_data.orders_online
where order_id is not null
    and order_amount > 0