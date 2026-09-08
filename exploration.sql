-- 1) Duplicate customers
SELECT customer_id, COUNT(*) AS n
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 2) Missing values
SELECT *
FROM vw_raw_customers
WHERE customer_id IS NULL
    OR full_name IS NULL
    OR email IS NULL
    OR phone IS NULL
    OR signup_date IS NULL;

SELECT *
FROM vw_raw_orders
WHERE order_id IS NULL
    OR customer_id IS NULL
    OR order_date IS NULL
    OR total_amount IS NULL
    OR currency IS NULL
    OR status IS NULL;

-- 3) Inconsistent phone formats
SELECT DISTINCT phone
FROM vw_raw_customers
WHERE phone IS NOT NULL;

-- 4) total_amount <= 0 (system errors)
SELECT order_id, total_amount, status
FROM vw_raw_orders
WHERE total_amount <= 0;

-- 5) customer_id ที่ vw_raw_orders ไม่มีอยู่จริงในตาราง customers
SELECT o.order_id, o.customer_id
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 6) orders ที่ order_date ไม่มี exchange rate ที่ตรงวันในตาราง vw_exchange_rates (ยกเว้น USD)
SELECT o.order_id, o.order_date, o.currency
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates e ON o.currency = e.currency AND o.order_date = e.date
WHERE o.currency IS NOT NULL AND o.currency <> 'USD' AND e.rate_to_usd IS NULL;