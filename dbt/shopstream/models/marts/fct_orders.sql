-- Fact table. Grain: one row per ORDER ITEM (declared, and tested via the
-- unique key). Revenue uses unit_price_at_purchase — the price actually
-- paid — not the product's current price.
select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_status,
    o.currency,
    oi.quantity,
    oi.unit_price_at_purchase,
    oi.quantity * oi.unit_price_at_purchase as revenue,
    o.order_created_at
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o using (order_id)
