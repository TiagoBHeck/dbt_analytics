{{
    config(
        materialized='view'
    )
}}

WITH movements AS (
  SELECT movements.*,
         _times.* 
    FROM {{ source('postgres', 'pix_movements') }} AS movements
    INNER JOIN {{ source('postgres', 'd_time') }} AS _times
      ON movements.pix_requested_at = _times.time_id
),

transfer_ins AS (
  SELECT transfers.*,
         _times.*
    FROM {{ source('postgres', 'transfer_ins') }} AS transfers
    INNER JOIN {{ source('postgres', 'd_time') }} AS _times
      ON transfers.transaction_requested_at = _times.time_id
),

transfer_outs AS (
  SELECT transfers.*,
         _times.*
    FROM {{ source('postgres', 'transfer_outs') }} AS transfers
    INNER JOIN {{ source('postgres', 'd_time') }} AS _times
      ON transfers.transaction_requested_at = _times.time_id
),

customers AS (
  SELECT customers.*,
         locations.* 
    FROM {{ source('postgres', 'customers') }} AS customers
    INNER JOIN {{ ref('locations') }} AS locations ON customers.customer_city = locations.city_id
),

accounts AS (
  SELECT _accounts.* 
    FROM {{ source('postgres', 'accounts') }} AS _accounts
)

SELECT accounts.account_id,
       accounts.status,
       accounts.account_branch,
       customers.customer_id,
       customers.first_name,
       customers.last_name,
       customers.city,
       customers.state,
       customers.country,
       movements.pix_amount,
       movements.status AS movement_status,
       movements.action_timestamp AS movements_timestamp,
       movements.in_or_out,
       transfer_ins.amount AS transfer_ins_amount,
       transfer_ins.status AS transfer_ins_status,
       transfer_ins.action_timestamp AS transfer_ins_timestamp,
       transfer_outs.amount AS transfer_outs_amount,
       transfer_outs.status AS transfer_outs_status,
       transfer_outs.action_timestamp AS transfer_outs_timestamp
  FROM accounts
  LEFT JOIN customers ON accounts.customer_id = customers.customer_id
  LEFT JOIN movements ON accounts.account_id = movements.account_id
  LEFT JOIN transfer_ins ON accounts.account_id = transfer_ins.account_id
  LEFT JOIN transfer_outs ON accounts.account_id = transfer_outs.account_id