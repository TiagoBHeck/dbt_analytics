{{
    config(
        materialized='ephemeral'
    )
}}

SELECT _city.city_id,
       _city.city,
       _state.state,
       _country.country
  FROM {{ source('postgres', 'city') }} AS _city
  INNER JOIN {{ source('postgres', 'state') }} AS _state ON _city.state_id = _state.state_id
  INNER JOIN {{ source('postgres', 'country') }} AS _country ON _state.country_id = _country.country_id