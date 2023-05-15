with player_valuations_cte as (

    select * from {{ ref('base_player_valuations') }}

),
players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select
    player_valuations_cte.*,
    players_cte.current_club_id,
    clubs_cte.domestic_competition_id as player_club_domestic_competition_id

from player_valuations_cte

left join players_cte using(player_id)

left join clubs_cte
    on players_cte.current_club_id = clubs_cte.club_id
