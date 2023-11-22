-- Query the database to find out which months have produced the most sales. 
-- The query should return the following information:

-- +-------------+-------+
-- | total_sales | month |
-- +-------------+-------+
-- |   673295.68 |     8 |
-- |   668041.45 |     1 |
-- |   657335.84 |    10 |
-- |   650321.43 |     5 |
-- |   645741.70 |     7 |
-- |   645463.00 |     3 |
-- +-------------+-------+

SELECT 
    ROUND(SUM(product_quantity * product_price)::numeric, 2) AS total_sales, month
FROM 
    dim_date_times
INNER JOIN
    orders_table ON orders_table.date_uuid = dim_date_times.date_uuid
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY
    month
ORDER BY
    total_sales DESC
LIMIT 6;

-- Query Output On Centralised Database
-- "total_sales","month"
-- "673295.68","8"
-- "668041.45","1"
-- "657335.84","10"
-- "650321.43","5"
-- "645741.70","7"
-- "645463.00","3"
