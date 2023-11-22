-- The sales team wants to know which of the different store types 
-- is generated the most revenue so they know where to focus.
-- Find out the total and percentage of sales coming from each of the different store types.
-- The query should return:
-- +-------------+-------------+---------------------+
-- | store_type  | total_sales | percentage_total(%) |
-- +-------------+-------------+---------------------+
-- | Local       |  3440896.52 |               44.87 |
-- | Web portal  |  1726547.05 |               22.44 |
-- | Super Store |  1224293.65 |               15.63 |
-- | Mall Kiosk  |   698791.61 |                8.96 |
-- | Outlet      |   631804.81 |                8.10 |
-- +-------------+-------------+---------------------+
WITH store_type_sales AS (
    SELECT
        dim_stores_data.store_type AS store_type,
        ROUND(SUM(product_quantity * product_price)::numeric, 2) AS total_sales
    FROM 
        orders_table
    INNER JOIN 
        dim_stores_data ON dim_stores_data.store_code = orders_table.store_code
    INNER JOIN 
        dim_products ON dim_products.product_code = orders_table.product_code
    GROUP BY
        store_type
    ORDER BY
        total_sales DESC
), grand_total_sales AS (
    SELECT SUM(total_sales) AS grand_total
    FROM store_type_sales
) 
SELECT 
    store_type, 
    total_sales, 
    ROUND(total_sales * 100 / grand_total_sales.grand_total, 2) AS "percentage_total(%)"
FROM store_type_sales, grand_total_sales

-- Query Output On Centralised Database

-- "store_type","total_sales","percentage_total(%)"
-- "Local","3440896.52","44.56"
-- "Web Portal","1726547.05","22.36"
-- "Super Store","1224293.65","15.85"
-- "Mall Kiosk","698791.61","9.05"
-- "Outlet","631804.81","8.18"
