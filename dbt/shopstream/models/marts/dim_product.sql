-- Product dimension, v1: current state only (Type 1).
-- Becomes SCD Type 2 in a later phase, once we have change history.
select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    current_unit_price,
    is_active
from {{ ref('stg_products') }}
