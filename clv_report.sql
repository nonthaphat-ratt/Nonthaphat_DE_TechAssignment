SELECT 
    c.customer_id,
    c.full_name,
    COUNT(o.order_id) AS total_orders_placed,
    ROUND(COALESCE(SUM(o.usd_amount), 0.0), 2) AS lifetime_value_usd,
    COALESCE(strftime('%Y-%m', c.signup_date), 'Unknown') AS customer_cohort
FROM dim_customers c
LEFT JOIN fct_orders o 
    ON c.customer_id = o.customer_id 
   AND o.status = 'COMPLETED'
GROUP BY 
    c.customer_id,
    c.full_name,
    c.signup_date
ORDER BY 
    lifetime_value_usd DESC;