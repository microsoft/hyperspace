SELECT
  catalog_sales.cs_bill_customer_sk,
  store_sales.ss_customer_sk
FROM catalog_sales, store_sales
WHERE catalog_sales.cs_sold_date_sk = store_sales.ss_sold_date_sk
LIMIT 100
