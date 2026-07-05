-- Customer dimension, v1: current state only (Type 1).
-- GDPR-deleted customers simply vanish from here; facts keep their
-- customer_id with no matching dimension row — find them with a left join.
select
    customer_id,
    email,
    full_name,
    country_code,
    marketing_opt_in,
    customer_created_at
from {{ ref('stg_customers') }}
