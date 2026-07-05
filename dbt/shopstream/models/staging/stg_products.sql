-- Latest snapshot of the product catalog.
-- NOTE: unit_price here is the *current* price. Joining facts to it
-- misstates historical revenue — the SCD2 lesson arrives in a later phase.
select
    product_id,
    sku,
    name as product_name,
    category,
    subcategory,
    toDecimal64(unit_price, 2) as current_unit_price,
    is_active
from {{ source('raw_shopstream', 'products') }}
where ds = {{ latest_ds(source('raw_shopstream', 'products')) }}
