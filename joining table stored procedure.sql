-- For Z-Pipeline
CREATE PROCEDURE sp_mfg_join_table
AS
BEGIN

SELECT cust.customer_code, 
       CONCAT(cust.first_name, ' ', cust.last_name) AS fullname, 
       cust.country, 
       cust.email, 
       SUM(txn.sales_value) AS total_spend_amt, 
       COUNT(txn.order_no) AS total_order
  FROM [dbo].[mfg_cus] AS cust
  INNER JOIN [dbo].[mfg_txn] AS txn
  ON txn.customer_code = cust.customer_code
  --WHERE cust.customer_code = 'A149276'
  GROUP BY cust.customer_code, cust.first_name, cust.last_name, cust.country, cust.email
  HAVING SUM(txn.sales_value) > 0 AND COUNT(txn.order_no) > 5
  ORDER BY total_order DESC

END;