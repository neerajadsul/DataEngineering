-- The company is looking to increase its online sales.
-- They want to know how many sales are happening online vs offline.
-- Calculate how many products were sold and the amount of sales made 
-- for online and offline purchases.
-- You should get the following information:
-- +------------------+-------------------------+----------+
-- | numbers_of_sales | product_quantity_count  | location |
-- +------------------+-------------------------+----------+
-- |            26957 |                  107739 | Web      |
-- |            93166 |                  374047 | Offline  |
-- +------------------+-------------------------+----------+

SELECT COUNT(product_quantity) AS numbers_of_sales, SUM(product_quantity) AS product_quantity_count,
	CASE
    WHEN store_type LIKE 'Web%' THEN 'Web'
    ELSE 'Offline'
    END AS store_location
FROM dim_stores_data
    INNER JOIN orders_table ON orders_table.store_code = dim_stores_data.store_code
GROUP BY
    store_location;

-- Query Output On Centralised Database

-- "numbers_of_sales","product_quantity_count","store_location"
-- "26957","107739","Web"
-- "93166","374047","Offline"

