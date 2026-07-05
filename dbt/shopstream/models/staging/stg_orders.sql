-- Latest snapshot of orders.
-- Remember: status mutates in the OLTP source, so this view only shows each
-- order's state *as of the last extract* — history in between is lost.
-- That gap is exactly what CDC (phase 2) will close.
select
    order_id,
    customer_id,
    status as order_status,
    currency,
    toDecimal64(total_amount, 2) as total_amount,
    created_at as order_created_at,
    updated_at as order_updated_at
from {{ source('raw_shopstream', 'orders') }}
where ds = {{ latest_ds(source('raw_shopstream', 'orders')) }}
