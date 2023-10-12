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
