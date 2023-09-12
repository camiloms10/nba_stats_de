
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='incremental',
        partition_by={
            "field": "DATE_TRUNC(GAME_DATE, YEAR)"
        },
        cluster_by=['TEAM_ID', 'PLAYER_ID'],
        unique_key="ID"
    )
}}


select
cast (PLAYER_ID as integer) as PLAYER_ID,
cast (TEAM_ID as integer) as TEAM_ID,
cast (GAME_ID as integer) as GAME_ID,
cast (GAME_DATE as date) as GAME_DATE,
cast (WL as string) as WL,
cast (MIN as decimal) as MIN,
cast (FGM as integer) as FGM,
cast (FGA as integer) as FGA,
cast (FG_PCT as decimal) as FG_PCT,
cast (FG3M as integer) as FG3M,
cast (FG3A as integer) as FG3A,
cast (FG3_PCT as decimal) as FG3_PCT,
cast (FTM as integer) as FTM,
cast (FTA as integer) as FTA,
cast (FT_PCT as decimal) as FT_PCT,
cast (OREB as integer) as OREB,
cast (DREB as integer) as DREB,
cast (REB as integer) as REB,
cast (AST as integer) as AST,
cast (TOV as integer) as TOV,
cast (STL as integer) as STL,
cast (BLK as integer) as BLK,
cast (BLKA as integer) as BLKA,
cast (PF as integer) as PF,
cast (PFD as integer) as PFD,
cast (PTS as integer) as PTS,
cast (PLUS_MINUS as integer) as PLUS_MINUS,
concat(PLAYER_ID,GAME_ID,TEAM_ID) as ID,
cast (GAME_TYPE as string) as GAME_TYPE,
cast (SEASON as string) as SEASON_YEAR,
current_timestamp() as LAST_UPDATE
from {{ source('staging','players_game_stats_current_season') }} 

UNION ALL

select
cast (PLAYER_ID as integer) as PLAYER_ID,
cast (TEAM_ID as integer) as TEAM_ID,
cast (GAME_ID as integer) as GAME_ID,
cast (GAME_DATE as date) as GAME_DATE,
cast (WL as string) as WL,
cast (MIN as decimal) as MIN,
cast (FGM as integer) as FGM,
cast (FGA as integer) as FGA,
cast (FG_PCT as decimal) as FG_PCT,
cast (FG3M as integer) as FG3M,
cast (FG3A as integer) as FG3A,
cast (FG3_PCT as decimal) as FG3_PCT,
cast (FTM as integer) as FTM,
cast (FTA as integer) as FTA,
cast (FT_PCT as decimal) as FT_PCT,
cast (OREB as integer) as OREB,
cast (DREB as integer) as DREB,
cast (REB as integer) as REB,
cast (AST as integer) as AST,
cast (TOV as integer) as TOV,
cast (STL as integer) as STL,
cast (BLK as integer) as BLK,
cast (BLKA as integer) as BLKA,
cast (PF as integer) as PF,
cast (PFD as integer) as PFD,
cast (PTS as integer) as PTS,
cast (PLUS_MINUS as integer) as PLUS_MINUS,
concat(PLAYER_ID,GAME_ID,TEAM_ID) as ID,
cast (GAME_TYPE as string) as GAME_TYPE,
cast (SEASON as string) as SEASON_YEAR,
current_timestamp() as LAST_UPDATE
from {{ source('staging','players_game_stats_past') }}

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses 'not in' to include records that are new playerid-gameid-teamid combinations)
  where concat(PLAYER_ID,GAME_ID,TEAM_ID) not in(select distinct ID from{{ this }})

{% endif %}


{% if var('is_test_run', default=true) %}
    limit 500
{% endif %}