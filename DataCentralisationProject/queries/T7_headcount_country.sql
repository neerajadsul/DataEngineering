-- The operations team would like to know the overall staff numbers 
-- in each location around the world. 
-- Perform a query to determine the staff numbers in each of 
-- the countries the company sells in.
-- The query should return the values:
-- +---------------------+--------------+
-- | total_staff_numbers | country_code |
-- +---------------------+--------------+
-- |               13307 | GB           |
-- |                6123 | DE           |
-- |                1384 | US           |
-- +---------------------+--------------+

SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM 
    dim_stores_data
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;


-- Query Output On Centralised Database

-- "total_staff_numbers","country_code"
-- "13132","GB"
-- "6054","DE"
-- "1304","US"
