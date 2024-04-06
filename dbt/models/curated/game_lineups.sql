{{
  config(
    enabled = false,
    )
}}
with game_lineups_cte as (

    select * from {{ ref('base_game_lineups') }}

),
games_cte as (

    select * from {{ ref('base_games') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'game_lineups_cte.game_id',
        'game_lineups_cte.player_id',
        'game_lineups_cte.type',
        'game_lineups_cte.position',
        'game_lineups_cte.number',
        'game_lineups_cte.team_captain'
    ]) }} as game_lineups_id,
    games_cte.date,
    game_lineups_cte.game_id,
    game_lineups_cte.player_id,
    game_lineups_cte.club_id,
    game_lineups_cte.player_name,
    game_lineups_cte."type",
    game_lineups_cte.position,
    game_lineups_cte."number",
    game_lineups_cte.team_captain

from game_lineups_cte

left join games_cte on game_lineups_cte.game_id = games_cte.game_id

order by game_lineups_cte.game_id, player_id, position, "number", team_captain, "type"
