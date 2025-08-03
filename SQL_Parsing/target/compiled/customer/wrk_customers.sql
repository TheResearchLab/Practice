-- Compiled wrk_customers model (work layer with business logic)
with customer_base as (
    select 
        customer_id,
        customer_name,
        email,
        phone,
        address,
        city,
        state,
        zip_code,
        registration_date,
        customer_tier,
        case 
            when customer_tier = 'PREMIUM' then 1
            else 0
        end as is_premium_customer,
        updated_at
    from ph_idea_play.aar58466.stg_customers
    where customer_id is not null
),

customer_metrics as (
    select 
        customer_id,
        sum(order_amount) as lifetime_value,
        max(order_date) as last_order_date,
        count(*) as total_orders
    from ph_idea_play.aar58466.stg_orders_online
    group by customer_id
    
    union all
    
    select 
        customer_id,
        sum(order_amount) as lifetime_value,
        max(order_date) as last_order_date,
        count(*) as total_orders
    from ph_idea_play.aar58466.stg_orders_retail
    group by customer_id
    
    union all
    
    select 
        customer_id,
        sum(order_amount) as lifetime_value,
        max(order_date) as last_order_date,
        count(*) as total_orders
    from ph_idea_play.aar58466.stg_orders_mobile
    group by customer_id
)

select 
    cb.customer_id,
    cb.customer_name,
    cb.email,
    cb.phone,
    cb.address,
    cb.city,
    cb.state,
    cb.zip_code,
    cb.registration_date,
    cb.customer_tier,
    cb.is_premium_customer,
    coalesce(sum(cm.lifetime_value), 0) as lifetime_value,
    max(cm.last_order_date) as last_order_date,
    coalesce(sum(cm.total_orders), 0) as total_orders,
    cb.updated_at
from customer_base cb
left join customer_metrics cm on cb.customer_id = cm.customer_id
group by cb.customer_id, cb.customer_name, cb.email, cb.phone, cb.address, 
         cb.city, cb.state, cb.zip_code, cb.registration_date, cb.customer_tier, 
         cb.is_premium_customer, cb.updated_at