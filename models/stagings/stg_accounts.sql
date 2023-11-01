{{ config(
  materialized='table',
  tags=['painel_contas']
) }}

SELECT * 
FROM {{ source('postgres', 'accounts') }}
ORDER BY created_at DESC
LIMIT 100