-- @block Return the ids, title and release year of all films which have the category 'Animation'
SELECT
	film_category.film_id,
	film.title,
	film.release_year
FROM 
	film_category
INNER JOIN
	film ON film.film_id = film_category.film_id
WHERE
	category_id IN
	(
		SELECT
			category.category_id
		FROM
			category
		WHERE
			category."name" = 'Animation'
	);


-- @block Return the first name, last name, and email of all customers in Canada
SELECT
	customer_id,
	CONCAT(c.first_name, c.last_name),
	c.email
FROM
	customer c
INNER JOIN
	address ON address.address_id = c.address_id
WHERE
	address.city_id IN
	(
		SELECT
			city_id
		FROM
			city
		WHERE
			city.country_id IN
			(
				SELECT
					country_id
				FROM
					country
				WHERE 
					country.country = 'Canada'
			)
	)

-- @block Return the titles of films with movies starting with A or I 
-- and rated not anything but G

SELECT
	title,
	rating
FROM
	film
WHERE
	(film.title LIKE 'A%'
	OR
	film.title LIKE 'I%')
	AND
	film.rating NOT IN
	(
		SELECT
			rating
		FROM
			film
		WHERE
			rating != 'G'
	)


-- @block Return the average film length per rating
SELECT
	ROUND(AVG(length),1) as avg_duration,
	rating
FROM
	film
GROUP BY
	rating


-- @block Find the average number of sales per day for each staff
SELECT
	staff_id,
	ROUND(SUM(daily_sell)/COUNT(daily_sell)) AS avg_sales_per_day_per_staff
FROM
	(
		SELECT
			COUNT(payment.rental_id) as daily_sell,
			DATE_TRUNC('day', payment_date) AS day_of_payment,
			staff_id
		FROM
			payment
		GROUP BY
			day_of_payment, staff_id
	) AS daily_sales_by_staff
GROUP BY
	staff_id
