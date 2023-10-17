{{
    config(
        materialized='view'
    )
}}


WITH movements_analytics AS (

  SELECT movements.*, 
         stats.min_amount,
         stats.max_amount,
         stats.total_amount,
         stats.avg_amount,
         stats.count_movements,
         stats.median_amount        
    FROM {{ source('postgres', 'pix_movements')}} AS movements
    INNER JOIN {{ ref('movements_statistics') }} AS stats ON movements.account_id = stats.account_id
    LEFT JOIN {{ source('postgres', 'accounts')}} AS contas ON movements.account_id = contas.account_id
  ORDER BY stats.total_amount DESC  

)

SELECT * FROM movements_analytics