with players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select
    players_cte.*,
    clubs_cte.domestic_competition_id as current_club_domestic_competition_id,
    clubs_cte.name as current_club_name,
    {# clubs_cte.highest_market_value as highest_market_value_in_eur #}
    {# clubs_cte.market_value as market_value_in_eur #}

from players_cte

left join clubs_cte on players_cte.current_club_id = clubs_cte.club_id
