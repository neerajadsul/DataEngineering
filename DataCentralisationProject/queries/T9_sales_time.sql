-- Sales would like the get an accurate metric for how 
-- quickly the company is making sales.

-- Determine the average time taken between each sale grouped by year, 
-- the query should return the following information:

--  +------+-------------------------------------------------------+
--  | year |                           actual_time_taken           |
--  +------+-------------------------------------------------------+
--  | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
--  | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
--  | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
--  | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
--  | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
--  +------+-------------------------------------------------------+

WITH tstamps AS (
    SELECT
        year,
        TO_TIMESTAMP(CONCAT(year,'-', month, '-', day,' ', timestamp), 'YYYY-MM-DD HH24:MI:SS')::timestamp AS ts
    FROM
        dim_date_times
    ORDER BY
        ts
), td as (
SELECT
    year,
    LEAD(ts) OVER (ORDER BY ts) - ts AS time_delay
FROM
    tstamps
) 
SELECT
    year,
    AVG(time_delay) AS actual_time_taken
FROM
    td
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT 5;



-- Query Output On Centralised Database
-- "year","actual_time_taken"
-- "2013","{""hours"":2,""minutes"":17,""seconds"":15,""milliseconds"":655.442}"
-- "1993","{""hours"":2,""minutes"":15,""seconds"":40,""milliseconds"":129.515}"
-- "2002","{""hours"":2,""minutes"":13,""seconds"":49,""milliseconds"":478.228}"
-- "2008","{""hours"":2,""minutes"":13,""seconds"":3,""milliseconds"":532.442}"
-- "2022","{""hours"":2,""minutes"":13,""seconds"":2,""milliseconds"":3.698}"
