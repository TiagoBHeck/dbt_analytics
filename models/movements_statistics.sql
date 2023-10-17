{{
    config(
        materialized='ephemeral'
    )
}}

WITH movements_stats AS (

  SELECT account_id, 
         sum(pix_amount) as total_amount,
         min(pix_amount) as min_amount,
         max(pix_amount) as max_amount,
         avg(pix_amount) as avg_amount,
         count(id) as count_movements,
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pix_amount) AS median_amount 
  FROM {{ source('postgres', 'pix_movements') }}
  GROUP BY account_id 
  ORDER BY account_id 

)

SELECT * FROM movements_stats