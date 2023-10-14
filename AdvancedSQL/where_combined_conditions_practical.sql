
    -- @block Find all the cities which begin with “al”
    SELECT
        city
    FROM
        city
    WHERE
        city LIKE 'al%';
    -- @block Find all the actors whose first name doesn't end in “EN”
    SELECT
        first_name
    FROM
        actor
    WHERE
        NOT first_name LIKE '%_EN';

    -- @block Find all the actors whose first name doesn't end in “EN” and have an ID greater than 100
    SELECT
        first_name,
        actor_id
    FROM
        actor
    WHERE
        NOT first_name LIKE '%_EN' AND actor_id > 100;
    -- @block Find all the actors whose first name doesn't end in “EN”, have an ID greater than 100, and have a last name that ends in “D”. Order the results by the last name in descending order.
    SELECT
        first_name,
        last_name,
        actor_id
    FROM
        actor
    WHERE
        NOT first_name LIKE '%_EN'
        AND 
        actor_id > 100
        AND
        last_name LIKE '%D'
    ORDER BY
        last_name DESC;

    -- @block Return only the address of addresses in either the Alberta or QLD district
    SELECT
        address,
        district
    FROM
        address
    WHERE
        district IN ('Alberta', 'QLD');
    -- @block Find actors who's first name begins with “MI” or last name ends with “ING”
    SELECT
        first_name,
        last_name
    FROM
        actor
    WHERE
        first_name LIKE 'MI%'
        OR
        last_name LIKE '%ING';
    -- @block What's the difference in results if you use an AND instead of an OR?
    -- @block Find actors who's first name begins with “MI” or last name ends with “ING”
    SELECT
        first_name,
        last_name
    FROM
        actor
    WHERE
        first_name LIKE 'MI%'
        AND
        last_name LIKE '%ING';
    -- @block Return film titles and descriptions of films whose lengths are between 80 and 100 minutes,
    SELECT
        title,
        length,
        description
    FROM
        film
    WHERE
        length BETWEEN '80' AND '100';
    -- @block Return film titles and descriptions of films whose lengths are between 80 and 100 minutes. Further filter these results by films which have a rental period between 5 and 7 days or have a replacement cost between $17 and $22
    SELECT
        title,
        length,
        description,
        rental_duration,
        replacement_cost
    FROM
        film
    WHERE
        length BETWEEN '80' AND '100'
        AND
        rental_duration BETWEEN '5' AND '7'
        AND
        replacement_cost BETWEEN '17' and '22';