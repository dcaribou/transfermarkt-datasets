with appearances_cte as (

    select * from {{ ref('base_appearances') }}

),
games_cte as (

    select * from {{ ref('base_games') }}

),
players_cte as (

    select * from {{ ref('base_players') }}

)

select
    appearances_cte.*,
    games_cte.*,
    players_cte.name as player_name,
    players_cte.current_club_id as player_current_club_id

from appearances_cte
left join games_cte using(game_id)
left join players_cte using(player_id)
