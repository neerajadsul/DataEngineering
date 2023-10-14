-- @block customers who rented a film

SELECT
	first_name,
	last_name,
	customer.customer_id,
	address.address,
	address.district,
	city.city
FROM
	customer
INNER JOIN
	payment ON payment.customer_id = customer.customer_id
INNER JOIN
	address ON address.address_id = customer.address_id
INNER JOIN
	city ON city.city_id = address.city_id
WHERE
	payment.amount > 0;


-- @block customers with rental within a time frame
SELECT
	first_name,
	last_name,
	customer.customer_id,
	address.address,
	address.district,
	city.city
FROM
	customer
INNER JOIN
	payment ON payment.customer_id = customer.customer_id
INNER JOIN
	address ON address.address_id = customer.address_id
INNER JOIN
	city ON city.city_id = address.city_id
WHERE
	payment.amount > 0
	AND
	payment.payment_date BETWEEN '2022-05-26' AND '2022-05-29'
ORDER BY
	last_name
LIMIT
	25;