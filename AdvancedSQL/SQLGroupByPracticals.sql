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


-- @block How many copies of the film 'HUNCHBACK IMPOSSIBLE' exist in the inventory system? 
SELECT
	film.film_id,
	film.title,
	COUNT(inventory.film_id)
FROM
	film
INNER JOIN
	inventory ON inventory.film_id = film.film_id
GROUP BY
	film.film_id
HAVING
	film.title = 'HUNCHBACK IMPOSSIBLE'


-- @block Find the total income per store. 
-- Return the income per store, the first line of the store's address 
-- and the first and last name of the store manager.
SELECT
	store.store_id,
	staff.first_name AS mgr_firstname,
	staff.last_name AS mgr_lastname,
	SUM(payment.amount),
	address.address
FROM
	store
INNER JOIN
	inventory ON inventory.store_id = store.store_id
INNER JOIN
	rental ON rental.inventory_id = inventory.inventory_id
INNER JOIN
	payment ON payment.rental_id = inventory.inventory_id
INNER JOIN
	staff ON staff.staff_id = store.manager_staff_id
INNER JOIN
	address ON address.address_id = store.address_id
GROUP BY
	store.store_id, mgr_firstname, mgr_lastname, address.address;


-- @block Return the names of the cities, along with the total amount spent, 
-- where over $149 has been spent over the course of the resident's membership. 
-- Order the results alphabetically on the city name
SELECT
	SUM(payment.amount) AS citywide_spending,
	city.city
FROM
	payment
INNER JOIN
	customer ON customer.customer_id = payment.customer_id
INNER JOIN
	address ON address.address_id = customer.address_id
INNER JOIN
	city ON city.city_id = address.city_id
GROUP BY
	city.city
HAVING
	SUM(payment.amount) > 149
ORDER BY
	city.city;


-- @block Return a table which counts the number transactions with
-- a low, medium, or high value transaction. 
-- A low payment is anything under $2, 
-- a medium anything between $2 and $7, 
-- and a high order anything above $6.

SELECT
	COUNT(payment.payment_id),
	CASE
		WHEN payment.amount < 2 THEN 'low'
		WHEN payment.amount > 2 AND payment.amount <= 7 THEN 'med'
		WHEN payment.amount > 6 THEN 'high'
	END AS transaction_value
FROM
	payment
GROUP BY
	transaction_value;



-- @block Return a table which counts the number of customers making 
-- a low, medium, or high value transaction. 
-- A low payment is anything under $2, 
-- a medium anything between $2 and $7, 
-- and a high order anything above $6.

SELECT
	COUNT(DISTINCT(payment.customer_id)),
	CASE
		WHEN payment.amount < 2 THEN 'low'
		WHEN payment.amount > 2 AND payment.amount <= 7 THEN 'med'
		WHEN payment.amount > 6 THEN 'high'
	END AS transaction_value
FROM
	payment
GROUP BY
	transaction_value;



-- @block List the last names of actors, as well as how many actors have that last name.
SELECT
	COUNT(actor.last_name) as lastname_frequency,
	actor.last_name AS lastname
FROM
	actor
GROUP BY
	lastname
ORDER BY
	lastname_frequency DESC;
	

-- @block How many customers are there per store? Return the number of customers and the store id.
SELECT
	COUNT(customer.customer_id) AS number_of_customers,
	customer.store_id
FROM
	customer
GROUP BY
	customer.store_id;



-- @block What was the largest order placed per customer? 
-- Return the customer ids and the amounts
SELECT
	MAX(payment.amount) as max_amount_order,
	payment.customer_id
FROM
	payment
GROUP BY
	customer_id
ORDER BY
	customer_id