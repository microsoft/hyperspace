SELECT
  date_dim.d_year,
  store_sales.ss_customer_sk
FROM date_dim, store_sales
WHERE date_dim.d_date_sk = store_sales.ss_sold_date_sk
LIMIT 100
