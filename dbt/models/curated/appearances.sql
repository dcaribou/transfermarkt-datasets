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
    appearances_cte.appearance_id,
    appearances_cte.game_id,
    appearances_cte.player_id,
    appearances_cte.player_club_id,
    coalesce(players_cte.current_club_id, -1) as player_current_club_id,
    games_cte.date,
    players_cte.name as player_name,
    appearances_cte.competition_id,
    appearances_cte.yellow_cards,
    appearances_cte.red_cards,
    appearances_cte.goals,
    appearances_cte.assists,
    appearances_cte.minutes_played
    
from appearances_cte
left join games_cte using(game_id)
left join players_cte using(player_id)

order by games_cte."date", appearances_cte.appearance_id
