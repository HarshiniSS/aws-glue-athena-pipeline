-- Amazon Athena queries run against the Glue Data Catalog database: jaffle_db
-- Data source: AwsDataCatalog   Database: jaffle_db

-- 1. Preview raw data
SELECT * FROM orders LIMIT 10;

-- 2. Row counts per table (expected: customers 100, orders 99, payments 113)
SELECT 'customers' AS tbl, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'payments', COUNT(*) FROM payments;

-- 3. Orders by status
SELECT status, COUNT(*) AS num_orders
FROM orders
GROUP BY status
ORDER BY num_orders DESC;

-- 4. Revenue by payment method (amount is stored in cents)
SELECT payment_method,
       COUNT(*)            AS num_payments,
       SUM(amount) / 100.0 AS total_usd
FROM payments
GROUP BY payment_method
ORDER BY total_usd DESC;

-- 5. Orders per month (order_date was inferred as string, so CAST it)
SELECT date_trunc('month', CAST(order_date AS date)) AS order_month,
       COUNT(*) AS num_orders
FROM orders
GROUP BY 1
ORDER BY 1;

-- 6. Customers with the most orders (2-table join)
SELECT c.first_name, c.last_name, COUNT(o.id) AS num_orders
FROM customers c
JOIN orders o ON o.user_id = c.id
GROUP BY c.first_name, c.last_name
ORDER BY num_orders DESC
LIMIT 5;

-- 7. Top spenders (3-table join)
SELECT c.first_name, c.last_name,
       SUM(p.amount) / 100.0 AS total_spent_usd
FROM customers c
JOIN orders   o ON o.user_id  = c.id
JOIN payments p ON p.order_id = o.id
GROUP BY c.first_name, c.last_name
ORDER BY total_spent_usd DESC
LIMIT 5;

-- Bonus: include customers with zero orders (LEFT JOIN)
SELECT c.first_name, c.last_name, COUNT(o.id) AS num_orders
FROM customers c
LEFT JOIN orders o ON o.user_id = c.id
GROUP BY c.first_name, c.last_name
ORDER BY num_orders ASC;
