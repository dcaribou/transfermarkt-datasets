with market_value_development_cte as (

    select * from {{ ref('base_market_value_development') }}

),
players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select
    market_value_development_cte.player_id,
    market_value_development_cte."date",
    market_value_development_cte.market_value_in_eur,
    players_cte.current_club_id,
    clubs_cte.domestic_competition_id as player_club_domestic_competition_id

from market_value_development_cte

left join players_cte using(player_id)

left join clubs_cte
    on players_cte.current_club_id = clubs_cte.club_id

order by "date", player_id
