-- @block Return 10 films who's lengths are under 120 mins
SELECT
    title,
    length,
    release_year
FROM
    film
WHERE
    length < 120
ORDER BY
    length DESC
LIMIT
    10;


-- @block Return the 10 longest films who's rating are G 
SELECT
    title,
    rating,
    length
FROM
    film
WHERE
    rating IN ('G')
ORDER BY
    length DESC
LIMIT 10;


-- @block Return all transactions where payment has been above $10
SELECT
    title,
    (rental_rate * rental_duration) AS cost_of_rental
FROM
    film
WHERE
    (rental_rate * rental_duration) > 10.0
LIMIT
    50;
-- @block Return the (replacement) cost per minute of every movie
SELECT
    title,
    ROUND(replacement_cost / rental_duration, 3) as replacement_cost_per_minute
FROM
    film;

-- @block Return the top 10 most expensive films to rent, 
-- based on the rental rate per hour of the movie
SELECT
    title,
    ROUND(rental_rate / length, 3) as rental_per_unit_length
FROM
    film
ORDER BY
    rental_per_unit_length DESC
LIMIT
    10;


