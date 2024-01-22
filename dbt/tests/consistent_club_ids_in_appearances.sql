{#
    Sanity check to ensure that home and away club IDs are consistent within appearances.
    This test tries to catch instances of the inconsitency reported in https://github.com/dcaribou/transfermarkt-datasets/issues/277.

#}

with appearances_cte as (

    select * from {{ ref('appearances') }}

),
games_cte as (

    select * from {{ ref('games') }}

)
-- get game IDs and club IDs from appearances and ensure that
-- for each game ID, the clubs ID in appearances is either the home or away club ID from games

select * 
from appearances_cte
left join games_cte on appearances_cte.game_id = games_cte.game_id
where appearances_cte.player_club_id not in (games_cte.home_club_id, games_cte.away_club_id)
