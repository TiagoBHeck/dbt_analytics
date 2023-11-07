{{
    config(
        materialized='view'
    )
}}

-- STEP 1: IMPORTS
WITH customers AS (
  SELECT * FROM {{ source('postgres', 'customers') }}
),

locations AS (
  SELECT * FROM {{ ref('locations') }}
),


-- STEP 2: CUSTOM LOGIC
customers_analytics_summarized AS (
  SELECT customers.customer_id, 
         concat(customers.first_name, ' ', customers.last_name) as customer_name, 
         locations.city,
         locations.state,
         locations.country,                 
         stats.min_amount,
         stats.max_amount,
         stats.total_amount,
         stats.avg_amount,
         stats.count_movements,
         stats.median_amount,
         stats.Total_pix_in,
         stats.Total_pix_in_amount,
         stats.Total_pix_out,
         stats.Total_pix_out_amount,
         stats.Total_completed,
         stats.Total_failed     
    FROM customers
    INNER JOIN {{ source('postgres', 'accounts') }} AS contas ON customers.customer_id = contas.customer_id
    INNER JOIN {{ source('postgres', 'city') }} AS city ON customers.customer_city = city.city_id
    INNER JOIN {{ ref('customers_statistics') }} AS stats ON contas.account_id = stats.account_id 
    INNER JOIN {{ ref('locations') }} AS locations ON customers.customer_city = locations.city_id 
),

customers_classifications AS (
  SELECT cus.*, 
    CASE WHEN cus.total_amount BETWEEN '0' AND '10000' THEN 'Classe C'
         WHEN cus.total_amount BETWEEN '10001' AND '99999' THEN 'Classe B'
         ELSE 'Classe A'
    END AS customer_class
  FROM customers_analytics_summarized AS cus
),

classes_analysis AS (
  SELECT 
    customer_class,
    sum(total_amount) as class_amount,
    avg(total_amount) as avg_class,
    count(customer_class) as count_class
  FROM customers_classifications
  GROUP BY customer_class
),


-- STEP 3: FINAL CTE
final AS (
  SELECT cust.*, 
         cla.avg_class,
      CASE 
        WHEN cust.customer_class = 'Classe A' AND cust.total_amount > cla.avg_class THEN 'Gold'
        WHEN cust.customer_class = 'Classe A' AND cust.total_amount < cla.avg_class THEN 'Ruby'
        WHEN cust.customer_class = 'Classe B' AND cust.total_amount > cla.avg_class THEN 'Silver'
        WHEN cust.customer_class = 'Classe B' AND cust.total_amount < cla.avg_class THEN 'Bronze'
        WHEN cust.customer_class = 'Classe C' THEN 'Copper'      
      END AS customer_type 
  FROM customers_classifications AS cust
  INNER JOIN classes_analysis AS cla ON (cust.customer_class = cla.customer_class)
)


-- STEP 4: SELECT FINAL
SELECT * FROM final

