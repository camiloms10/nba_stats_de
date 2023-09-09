{{ config(materialized='table') }}

select * from {{ ref('players_stats_all') }}