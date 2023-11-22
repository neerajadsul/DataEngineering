SELECT locality, COUNT(store_code) AS total_num_stores
FROM dim_stores_data
GROUP BY
    locality
ORDER BY
    total_num_stores DESC
LIMIT 10


-- Query Output On Centralised Database
-- "locality","total_num_stores"
-- "Chapletown","14"
-- "Belper","13"
-- "Bushey","12"
-- "Exeter","11"
-- "High Wycombe","10"
-- "Rutherglen","10"
-- "Arbroath","10"
-- "Surbiton","9"
-- "Lancing","9"
-- "Aberdeen","9"
