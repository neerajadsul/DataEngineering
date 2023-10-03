-- @block Get overview
SELECT *
FROM payments_2022_may
LIMIT 10;

-- @block Add new column
ALTER TABLE payments_2022_may
    ADD COLUMN payment_date_truncated TIMESTAMP; 


-- @block Remove milliseconds from payment date
UPDATE payments_2022_may
set payment_date = date_trunc('second', payment_date);


-- @block Remove milliseconds from payment date
UPDATE payments_2022_may
set payment_date_truncated = date_trunc('minute', payment_date);


-- @block Drop the payment_id column
ALTER TABLE payments_2022_may
DROP COLUMN payment_id;


-- @block Set the staff_id to 1 where amount = 3.99 and customer_id = 87 and 137
SELECT staff_id, rental_id, amount, customer_id
FROM payments_2022_may
WHERE
    amount = 3.99
    and
    customer_id IN ('87', '137')

-- @block Update staff id
UPDATE payments_2022_may
SET staff_id = '1'
WHERE
    amount = 3.99
    and
    customer_id IN ('87', '137');


-- @block There was a service charge of 50 pence added after '2007-03-22'. 
-- Update the amount columns rows with the service charge after this date.
UPDATE payments_2022_may
SET amount = amount + 0.50
WHERE
    payment_date > '2022-03-22';


-- @block Rename amount columns to total_payment_taken
ALTER TABLE payments_2022_may
RENAME COLUMN amount TO total_payment_taken;