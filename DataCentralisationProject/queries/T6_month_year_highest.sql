-- The company stakeholders want assurances that the company has 
-- been doing well recently.
-- Find which months in which years have had the most sales historically.
-- The query should return the following information:
-- +-------------+------+-------+
-- | total_sales | year | month |
-- +-------------+------+-------+
-- |    27936.77 | 1994 |     3 |
-- |    27356.14 | 2019 |     1 |
-- |    27091.67 | 2009 |     8 |
-- |    26679.98 | 1997 |    11 |
-- |    26310.97 | 2018 |    12 |
-- |    26277.72 | 2019 |     8 |
-- |    26236.67 | 2017 |     9 |
-- |    25798.12 | 2010 |     5 |
-- |    25648.29 | 1996 |     8 |
-- |    25614.54 | 2000 |     1 |
-- +-------------+------+-------+

-- For each year find the month with highest sales 
SELECT
    ROUND(SUM(product_quantity * product_price)::numeric,2) AS total_sales,
    dim_date_times.year,
    dim_date_times.month
FROM orders_table
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
GROUP BY
    dim_date_times.year, dim_date_times.month
ORDER BY
    total_sales DESC
LIMIT 10;


-- Query Output On Centralised Database
-- "total_sales","year","month"
-- "27936.77","1994","3"
-- "27356.14","2019","1"
-- "27091.67","2009","8"
-- "26679.98","1997","11"
-- "26310.97","2018","12"
-- "26277.72","2019","8"
-- "26236.67","2017","9"
-- "25798.12","2010","5"
-- "25648.29","1996","8"
-- "25614.54","2000","1"
