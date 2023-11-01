{{ config(
  materialized='table',
  tags=['painel_contas']
) }}


{% set pix_status = ["failed", "completed"] %}

SELECT id,
       in_or_out,
       status,
  {% for _status in pix_status %}
  SUM(CASE WHEN status = '{{_status}}' THEN pix_amount END) AS {{_status}}_amount
  {% if not loop.last %},{% endif %}
  {% endfor %}
  FROM {{ source('postgres', 'pix_movements')}}
  GROUP BY 1, 2, 3  
  LIMIT 100