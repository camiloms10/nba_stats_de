{{ config(materialized='table',
    )
}}

select
cast (id as integer) as ID,
cast (full_name as string) as FULL_NAME,
cast (abbreviation as string) as ABBREVIATION,
cast (nickname as string) as NICKNAME,
cast (city as string) as CITY,
cast (state as string) as STATE,
cast (year_founded as integer) as YEAR_FOUNDED,
current_timestamp() as LAST_UPDATE
from {{ source('staging','nba_teams') }} 

{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}