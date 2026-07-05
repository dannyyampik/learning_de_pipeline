-- Staging is where types get fixed: money arrives from the lake as Float64
-- (a classic Parquet-inference trap); cast to Decimal before it hits a mart.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    toDecimal64(unit_price_at_purchase, 2) as unit_price_at_purchase
from {{ source('raw_shopstream', 'order_items') }}
where ds = {{ latest_ds(source('raw_shopstream', 'order_items')) }}
