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
