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


-- @ Return the total sales per day, along with the number of movies rented for that day
SELECT
	SUM(amount) AS day_amount,
	DATE_TRUNC('day', payment_date) as day_of_payment,
	COUNT(rental_id) as rentals_on_day
FROM
	payment
GROUP BY
	day_of_payment
ORDER BY
	day_of_payment;


-- @block ind the id's of all customers who have spent over $100 
-- during the course of their membership
SELECT
	customer_id,
	SUM(amount) AS total
FROM
	payment
GROUP BY
	customer_id
HAVING
	SUM(amount) > 100.0
ORDER BY
	total;


-- @block Find the average amount spent per film rating. 
-- Return the film rating and the amount.
SELECT
	film.rating,
	ROUND(AVG(payment.amount), 2) AS avg_amount_rating
FROM
	payment
INNER JOIN
	rental ON rental.rental_id = payment.rental_id
INNER JOIN
	inventory ON inventory.inventory_id = rental.inventory_id
INNER JOIN
	film ON film.film_id = inventory.film_id
GROUP BY
	film.rating;


-- @block How many rented films have yet to be returned?
SELECT
	(COUNT(rental.rental_id) - COUNT(return_date)) AS films_to_return
FROM
	rental;


