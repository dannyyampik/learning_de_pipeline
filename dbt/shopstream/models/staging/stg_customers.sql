-- Latest snapshot of customers, renamed/typed for analytics use.
select
    customer_id,
    email,
    full_name,
    country_code,
    marketing_opt_in,
    created_at as customer_created_at
from {{ source('raw_shopstream', 'customers') }}
where ds = {{ latest_ds(source('raw_shopstream', 'customers')) }}
