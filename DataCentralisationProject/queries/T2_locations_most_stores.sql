-- The business stakeholders would like to know which locations 
-- currently have the most stores.
-- They would like to close some stores before opening more in other locations.
-- Find out which locations have the most stores currently. 
-- The query should return the following:

-- +-------------------+-----------------+
-- |     locality      | total_no_stores |
-- +-------------------+-----------------+
-- | Chapletown        |              14 |
-- | Belper            |              13 |
-- | Bushley           |              12 |
-- | Exeter            |              11 |
-- | High Wycombe      |              10 |
-- | Arbroath          |              10 |
-- | Rutherglen        |              10 |
-- +-------------------+-----------------+

SELECT locality, COUNT(store_code) AS total_num_stores
FROM dim_stores_data
GROUP BY
    locality
ORDER BY
    total_num_stores DESC, locality
LIMIT 7;

-- Query Output On Centralised Database
-- "locality","total_num_stores"
-- "Chapletown","14"
-- "Belper","13"
-- "Bushey","12"
-- "Exeter","11"
-- "Arbroath","10"
-- "High Wycombe","10"
-- "Rutherglen","10"

