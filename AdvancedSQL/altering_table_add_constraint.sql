-- @block Change column field TYPE
ALTER TABLE
    teenage_movies
ALTER COLUMN
    rating TYPE TEXT;


-- @block Drop a column from TABLE
ALTER TABLE teenage_movies
DROP COLUMN release_year;


-- @block Add a new column
ALTER TABLE teenage_movies
ADD COLUMN film_id INTEGER

-- @block Add FK constraint to film id
ALTER TABLE teenage_movies
ADD CONSTRAINT FK_film_id
FOREIGN KEY (film_id)
REFERENCES film(film_id);


-- @block Update the table using FK values
UPDATE teenage_movies
SET film_id = (
    SELECT
        film_id
    FROM film
    WHERE film.title = teenage_movies.title
);
-- @block Check updates
SELECT *
FROM
    teenage_movies
LIMIT 10;



