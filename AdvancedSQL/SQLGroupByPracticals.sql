-- @block Find the unique special features of films
SELECT
	DISTINCT(special_features)
FROM
	film
ORDER BY
	special_features;


-- @block Which 3 days of the week are most profitable for the business?
SELECT
	CASE
		WHEN EXTRACT('dow' from payment_date) = 0 THEN 'Sun'
		WHEN EXTRACT('dow' from payment_date) = 1 THEN 'Mon'
		WHEN EXTRACT('dow' from payment_date) = 2 THEN 'Tue'
		WHEN EXTRACT('dow' from payment_date) = 3 THEN 'Wed'
		WHEN EXTRACT('dow' from payment_date) = 4 THEN 'Thur'
		WHEN EXTRACT('dow' from payment_date) = 5 THEN 'Fri'
		WHEN EXTRACT('dow' from payment_date) = 6 THEN 'Sat'
	END AS day_of_week,
	SUM(amount) as dow_total_amount
FROM
	payment
GROUP BY
	day_of_week
ORDER BY
	dow_total_amount
LIMIT 3;

