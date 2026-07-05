-- Daily revenue from orders that were actually paid for.
select
    toDate(order_created_at) as order_date,
    uniqExact(order_id) as orders,
    sum(quantity) as units_sold,
    sum(revenue) as revenue
from {{ ref('fct_orders') }}
where order_status in ('paid', 'shipped', 'delivered')
group by order_date
order by order_date
