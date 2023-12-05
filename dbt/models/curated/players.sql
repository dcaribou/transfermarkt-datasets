with players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

),
players_market_value_cte as (

    select * from {{ ref('base_market_value_development') }}

)

select
    players_cte.*,
    clubs_cte.domestic_competition_id as current_club_domestic_competition_id,
    clubs_cte.name as current_club_name,
    players_market_value_cte.market_value_in_eur,
    players_market_value_cte.highest_market_value_in_eur,

from players_cte

left join clubs_cte on players_cte.current_club_id = clubs_cte.club_id
left join players_market_value_cte on players_cte.player_id = players_market_value_cte.player_id
