{% macro get_action_timestamp(time_id) %}
  
    select action_timestamp 
    from {{ source('postgres', 'd_time') }} 
    where time_id = {{time_id}}
   
{% endmacro %}