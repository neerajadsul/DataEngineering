-- The Operations team would like to know which countries 
-- we currently operate in and which country now has the most stores. 

-- Perform a query on the database to get the information, 
-- it should return the following information:
-- +----------+-----------------+
-- | country  | total_no_stores |
-- +----------+-----------------+
-- | GB       |             265 |
-- | DE       |             141 |
-- | US       |              34 |
-- +----------+-----------------+


SELECT country_code AS country, COUNT(store_code) AS total_num_stores
FROM 
	dim_stores_data
GROUP BY
	country_code
ORDER BY
	total_num_stores DESC

-- Query Output On Centralised Database
-- "country","total_num_stores"
-- "GB","266"
-- "DE","141"
-- "US","34"
