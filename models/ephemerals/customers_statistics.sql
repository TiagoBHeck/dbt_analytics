{{
    config(
        materialized='ephemeral'
    )
}}

WITH movements_stats_in AS (

  SELECT account_id,
         count(id) AS Total_pix_in,
         sum(pix_amount) AS Total_pix_in_amount
    FROM {{ source('postgres', 'pix_movements') }}
    WHERE in_or_out = 'pix_in'
    GROUP BY account_id
),

movements_stats_out AS (

  SELECT account_id,
         count(id) AS Total_pix_out,
         sum(pix_amount) AS Total_pix_out_amount
    FROM {{ source('postgres', 'pix_movements') }}
    WHERE in_or_out = 'pix_out'
    GROUP BY account_id
),

movements_stats_completed AS (

  SELECT account_id,
         count(id) AS Total_completed
    FROM {{ source('postgres', 'pix_movements') }}
    WHERE status = 'completed'
    GROUP BY account_id
),

movements_stats_failed AS (

  SELECT account_id,
         count(id) AS Total_failed
    FROM {{ source('postgres', 'pix_movements') }}
    WHERE status = 'failed'
    GROUP BY account_id
),

final AS (

  SELECT pix_movements.account_id, 
         movements_stats_in.Total_pix_in,
         movements_stats_in.Total_pix_in_amount,
         movements_stats_out.Total_pix_out,
         movements_stats_out.Total_pix_out_amount,
         movements_stats_completed.Total_completed,
         movements_stats_failed.Total_failed,
         sum(pix_movements.pix_amount) as total_amount,
         min(pix_movements.pix_amount) as min_amount,
         max(pix_movements.pix_amount) as max_amount,
         avg(pix_movements.pix_amount) as avg_amount,
         count(pix_movements.id) as count_movements,         
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pix_amount) AS median_amount
  FROM {{ source('postgres', 'pix_movements') }} AS pix_movements  
  INNER JOIN movements_stats_in ON pix_movements.account_id = movements_stats_in.account_id
  INNER JOIN movements_stats_out ON pix_movements.account_id = movements_stats_out.account_id
  INNER JOIN movements_stats_completed ON pix_movements.account_id = movements_stats_completed.account_id
  INNER JOIN movements_stats_failed ON pix_movements.account_id = movements_stats_failed.account_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT * FROM final