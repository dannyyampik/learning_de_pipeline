-- `make demo-olap` — what has landed in the warehouse.
SELECT 'raw snapshots loaded' AS check, table AS name, sum(rows) AS rows
FROM system.parts
WHERE database = 'raw' AND active
GROUP BY table
ORDER BY table;

SELECT 'mart' AS check, name, total_rows AS rows
FROM system.tables
WHERE database = 'analytics' AND engine LIKE '%MergeTree%'
ORDER BY name;

SELECT * FROM analytics.mart_daily_revenue ORDER BY order_date DESC LIMIT 7;
