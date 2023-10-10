-- @block How many rented films do not have a return date?

SELECT
-- count total rows with non-null columns and then subtract count of return_date
-- since count does not count NULL rows, we get total rentals without a return date
	(COUNT(rental.rental_date) - COUNT(rental.return_date))
FROM
	rental;


-- @block What is the total amount of payments that the business has received?
SELECT
	SUM(payment.amount) AS total_amount_payments,
	COUNT(payment.amount) AS total_number_of_payments,
	ROUND(AVG(payment.amount), 3) AS avg_amount_per_payment
FROM
	payment;

-- @block What is the total amount of payments that the business has received between 
-- the dates '2022-01-25' AND '2022-01-31'
SELECT
	SUM(payment.amount)
FROM
	payment
WHERE
	payment.payment_date BETWEEN '2022-01-25' AND '2022-01-31';

-- @block When was the earliest transaction made?
SELECT
	MIN(payment.payment_date)
FROM
	payment;


-- @block When was the last transaction over $10 made?
SELECT
	MAX(payment.payment_date) AS last_transaction_over_10
FROM
	payment
WHERE
	payment.amount > 10.0;

-- @block What is the price of the highest value film the business has?
-- I am assuming high value is maximum rental payments made film
SELECT	
	film.title,
	SUM(payment.amount) as total_amount_film,
	film.rental_rate
FROM
	rental
INNER JOIN
	payment ON payment.rental_id = rental.rental_id
INNER JOIN
	inventory ON inventory.inventory_id = rental.inventory_id
INNER JOIN
	film ON film.film_id = inventory.film_id
GROUP BY
	film.title, film.rental_rate
ORDER BY
	total_amount_film DESC
LIMIT 5;


-- @block What is the average length of films?
SELECT
	ROUND(AVG(film.length), 2) as avg_length_of_films
FROM
	film;


-- @block What is the average length of films who's rental cost is under $2.99
SELECT
	AVG(film.length)
FROM
	film
WHERE
	film.rental_rate < 2.99;



