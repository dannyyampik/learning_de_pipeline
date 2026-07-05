select
    payment_id,
    order_id,
    method as payment_method,
    status as payment_status,
    toDecimal64(amount, 2) as amount,
    created_at as payment_created_at
from {{ source('raw_shopstream', 'payments') }}
where ds = {{ latest_ds(source('raw_shopstream', 'payments')) }}
