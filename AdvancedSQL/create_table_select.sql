-- @block Teen Movies
CREATE TABLE IF NOT EXISTS teenage_movies AS (
    SELECT
        title,
        rating,
        description,
        release_year
    FROM
        film
    WHERE
        NOT rating IN ('R', 'NC-17')
);

-- @block Check Table
SELECT *
FROM teenage_movies
LIMIT 10;


-- @block Table of payments in May 2022
CREATE TABLE IF NOT EXISTS payments_may_2022 AS (
SELECT
    payment_date,
    payment_id,
    payment.amount,
    payment.customer_id,
    payment.rental_id,
    payment.staff_id
FROM
    payment
WHERE
    payment_date BETWEEN '2022-05-01' AND '2022-05-31'
ORDER BY
    payment_date ASC
);

-- @block Check Table
SELECT
    *
FROM
    payments_may_2022
ORDER BY
    payments_may_2022.amount ASC;
