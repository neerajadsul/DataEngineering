-- @block Detailed payments per film
--  Join the customer table to the payment table to get the names of customer who made payments
--  Join the rental to get the inventory_ids to match with films
--  Use the inventory table to get the film_ids
--  Finally join the film table to get the names of all films and the payment made for them

SELECT
	film.title,
	SUM(payment.amount) as total_amount
FROM
	payment
INNER JOIN
	customer ON customer.customer_id = payment.customer_id
INNER JOIN
	rental ON rental.rental_id = payment.rental_id
INNER JOIN
	inventory ON inventory.inventory_id = rental.inventory_id
INNER JOIN
	film ON film.film_id = inventory.film_id
GROUP BY
	film.title
ORDER BY
	total_amount DESC;


-- @block Checking Customer Rentals
-- Join the customer table to the rental table to check when customers were making rentals
-- Now join the inventory table to get the film_ids of the films rented
-- Now join the film table to get the names of all films rented by customers

SELECT
    film.title,
    film.film_id,
    customer.customer_id
FROM 
    customer
INNER JOIN
    rental ON rental.customer_id = customer.customer_id
INNER JOIN
    inventory ON inventory.inventory_id = rental.inventory_id
INNER JOIN
	film ON film.film_id = inventory.film_id;



-- @block Checking Customer Rentals
--  Join the address table to the customer` table to get the address of all customers
--  Now join the city table to get all cities for the address
--  Now join the country table to get all the countries
--  Filter the data to only get customer which live in the United States

SELECT
	customer.customer_id,
	customer.first_name,
	customer.last_name,
	address.district,
	address.postal_code,
	country.country
FROM
	customer
INNER JOIN
	address ON address.address_id = customer.address_id
INNER JOIN
	city ON city.city_id = address.city_id
INNER JOIN
	country ON country.country_id = city.country_id
WHERE
	country.country = 'United States';

