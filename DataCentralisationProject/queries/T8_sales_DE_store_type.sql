-- The sales team is looking to expand their territory in Germany. 
-- Determine which type of store is generating the most sales in Germany.
-- The query will return:
-- +--------------+-------------+--------------+
-- | total_sales  | store_type  | country_code |
-- +--------------+-------------+--------------+
-- |   198373.57  | Outlet      | DE           |
-- |   247634.20  | Mall Kiosk  | DE           |
-- |   384625.03  | Super Store | DE           |
-- |  1109909.59  | Local       | DE           |
-- +--------------+-------------+--------------+

SELECT
    ROUND(SUM(product_quantity * product_price)::numeric,2) AS total_sales,
    store_type,
    country_code
FROM
    orders_table
INNER JOIN
    dim_products ON dim_products.product_code = orders_table.product_code
INNER JOIN
    dim_stores_data ON dim_stores_data.store_code = orders_table.store_code
GROUP BY
    store_type, country_code
HAVING
    country_code = 'DE'
ORDER BY
    total_sales;



-- Query Output On Centralised Database
-- "total_sales","store_type","country_code"
-- "198373.57","Outlet","DE"
-- "247634.20","Mall Kiosk","DE"
-- "384625.03","Super Store","DE"
-- "1109909.59","Local","DE"
