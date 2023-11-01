{{
    config(
        materialized='ephemeral'
    )
}}

WITH d_time AS (

  SELECT * FROM {{ source('postgres', 'd_time')}}
),

d_week AS (

  SELECT * FROM {{ source('postgres', 'd_week')}}
),

d_month AS (

  SELECT * FROM {{ source('postgres', 'd_month')}}
),

d_year AS (

  SELECT * FROM {{ source('postgres', 'd_year')}}
),

d_weekday AS (

  SELECT * FROM {{ source('postgres', 'd_weekday')}}
),

final AS (

  SELECT d_time.time_id,
         d_week.action_week AS 'Week',
         d_month.action_month AS 'Month',
         d_year.action_year AS 'Year',
         d_weekday.action_weekday AS 'Weekday' 
    FROM d_time
    INNER JOIN d_week ON d_time.week_id = d_week.week_id
    INNER JOIN d_month ON d_time.month_id = d_month.month_id
    INNER JOIN d_year ON d_time.year_id = d_year.year_id
    INNER JOIN d_weekday ON d_time.weekday_id = d_weekday.weekday_id
)

SELECT * FROM final