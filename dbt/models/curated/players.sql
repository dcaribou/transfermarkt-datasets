with players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

),
players_market_value_cte as (

    select
        *,
        row_number() over (partition by player_id order by date desc) as rn

    from {{ ref('base_market_value_development') }}

),
-- get the current and maximum market value for each player
market_value_agg_cte as (

    select
        player_id,
        max(market_value_in_eur) as highest_market_value_in_eur,
        max(case when rn = 1 then market_value_in_eur else null end) as market_value_in_eur

    from players_market_value_cte

    group by player_id
)

select
    players_cte.*,
    clubs_cte.domestic_competition_id as current_club_domestic_competition_id,
    clubs_cte.name as current_club_name,
    market_value_agg_cte.market_value_in_eur,
    market_value_agg_cte.highest_market_value_in_eur,

from players_cte

left join clubs_cte on players_cte.current_club_id = clubs_cte.club_id

left join market_value_agg_cte on players_cte.player_id = market_value_agg_cte.player_id

order by players_cte.player_id
