{{
    config(
        materialized='view'
    )
}}

WITH customers_analytics AS (

  SELECT customers.customer_id, 
         customers.first_name,
         customers.last_name,
         city.city,
         customers.country_name,
         stats.min_amount,
         stats.max_amount,
         stats.total_amount,
         stats.avg_amount,
         stats.count_movements,
         stats.median_amount        
    FROM {{ source('postgres', 'customers') }} AS customers
    INNER JOIN {{ source('postgres', 'accounts') }} AS contas ON customers.customer_id = contas.customer_id
    INNER JOIN {{ source('postgres', 'city') }} AS city ON customers.customer_city = city.city_id
    INNER JOIN {{ ref('customers_statistics') }} AS stats ON contas.account_id = stats.account_id    
  ORDER BY stats.total_amount DESC  

)

SELECT * FROM customers_analytics