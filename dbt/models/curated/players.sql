with players_cte as (

    select * from {{ ref('base_players') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select
    players_cte.*,
    clubs_cte.domestic_competition_id as current_club_domestic_competition_id,
    clubs_cte.name as current_club_name

from players_cte

left join clubs_cte on players_cte.current_club_id = clubs_cte.club_id
