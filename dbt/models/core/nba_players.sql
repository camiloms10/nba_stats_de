{{ config(materialized='table',
        partition_by={
            "field": "ID"
        }
    )
}}

select
cast (id as integer) as ID,
cast (full_name as string) as FULL_NAME,
cast (first_name as string) as FIRST_NAME,
cast (last_name as string) as LAST_NAME,
cast (is_active as boolean) as IS_ACTIVE,
current_timestamp() as LAST_UPDATE
from {{ source('staging','nba_players') }} 

{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}
