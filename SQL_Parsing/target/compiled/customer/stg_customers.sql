-- Compiled stg_customers model (staging layer)
select
    customer_id,
    trim(customer_name) as customer_name,
    lower(email) as email,
    phone,
    address,
    city,
    state,
    zip_code,
    registration_date::date as registration_date,
    case 
        when annual_spend >= 10000 then 'PREMIUM'
        when annual_spend >= 5000 then 'GOLD'
        when annual_spend >= 1000 then 'SILVER'
        else 'BRONZE'
    end as customer_tier,
    current_timestamp() as updated_at
from raw_data_db.customer_data.customers
where customer_id is not null
    and email is not null