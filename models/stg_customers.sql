{{ config(
  materialized='table',
  tags=['painel_contas']
) }}

SELECT clientes.customer_id,
       clientes.first_name,
       clientes.customer_city,
       contas.account_id,
       contas.created_at,
       contas.status
  FROM {{ source('postgres', 'customers') }} AS clientes
  LEFT JOIN {{ source('postgres', 'accounts')}} AS contas
  ON clientes.customer_id = contas.customer_id
ORDER BY contas.created_at DESC
LIMIT 100