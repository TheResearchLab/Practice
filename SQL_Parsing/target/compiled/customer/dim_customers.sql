-- Compiled dim_customers model (built from snapshot)
with customer_enriched as (
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
        is_premium_customer,
        lifetime_value,
        last_order_date,
        total_orders,
        dbt_valid_from,
        dbt_valid_to,
        updated_at
    from ph_idea_play.aar58466.snp_customers
    where dbt_valid_to is null  -- Current records only
)

select 
    customer_id,
    customer_name,
    email,
    phone,
    concat(address, ', ', city, ', ', state, ' ', zip_code) as full_address,
    registration_date,
    customer_tier,
    is_premium_customer,
    lifetime_value,
    last_order_date,
    total_orders,
    case 
        when total_orders >= 50 then 'VIP'
        when total_orders >= 20 then 'LOYAL'
        when total_orders >= 5 then 'REGULAR'
        else 'NEW'
    end as customer_segment,
    updated_at
from customer_enriched