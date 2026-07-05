-- `make demo` — a quick pulse check on what the fictional app is doing.
\echo '=== Row counts ==='
SELECT 'customers' AS table_name, count(*) FROM customers
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'orders', count(*) FROM orders
UNION ALL SELECT 'order_items', count(*) FROM order_items
UNION ALL SELECT 'payments', count(*) FROM payments
ORDER BY 1;

\echo ''
\echo '=== Orders by status (watch these move between runs) ==='
SELECT status, count(*) AS orders, round(sum(total_amount), 2) AS amount
FROM orders GROUP BY status ORDER BY status;

\echo ''
\echo '=== Last 5 orders ==='
SELECT o.order_id, o.customer_id, o.status, o.total_amount, o.created_at::timestamp(0)
FROM orders o ORDER BY o.order_id DESC LIMIT 5;

\echo ''
\echo '=== Revenue by category (paid+ orders only) ==='
SELECT p.category,
       count(DISTINCT o.order_id)                       AS orders,
       round(sum(oi.quantity * oi.unit_price_at_purchase), 2) AS revenue
FROM orders o
JOIN order_items oi USING (order_id)
JOIN products p USING (product_id)
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY p.category ORDER BY revenue DESC;

\echo ''
\echo '=== Learning traps in action ==='
SELECT 'products whose price changed since creation' AS trap, count(*)
FROM products WHERE updated_at > created_at
UNION ALL
SELECT 'orphaned orders (GDPR-deleted customers)', count(*)
FROM orders WHERE customer_id IS NULL;
