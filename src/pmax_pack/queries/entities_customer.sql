SELECT
  customer.id AS account_id,
  customer.descriptive_name AS descriptive_name,
  customer.currency_code AS currency_code,
  customer.time_zone AS time_zone,
  customer.status AS status,
  customer.manager AS manager
FROM customer
