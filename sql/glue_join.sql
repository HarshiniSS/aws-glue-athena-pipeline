-- Glue Studio "SQL Query" transform (Spark SQL)
-- Input aliases: customers, orders, payments (one per Data Catalog source node)
-- COALESCE(1) merges the result into one partition so Spark writes a single output file.

SELECT /*+ COALESCE(1) */ *
FROM (
  SELECT o.id             AS order_id,
         o.order_date,
         o.status,
         c.id             AS customer_id,
         c.first_name,
         c.last_name,
         p.id             AS payment_id,
         p.payment_method,
         p.amount / 100.0 AS amount_usd
  FROM orders o
  JOIN customers c     ON o.user_id  = c.id
  LEFT JOIN payments p ON p.order_id = o.id
  ORDER BY order_id, payment_id
) t
