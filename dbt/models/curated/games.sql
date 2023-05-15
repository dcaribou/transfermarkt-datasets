with games_cte as (

    select * from {{ ref('base_games') }}

),
competitions_cte as (

    select * from {{ ref('base_competitions') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

)

select
    games_cte.*,
    home_clubs_cte.name as home_club_name,
    away_clubs_cte.name as away_club_name,
    games_cte.home_club_goals || ':' || games_cte.away_club_goals as "aggregate",
    competitions_cte.type as competition_type

from games_cte

left join competitions_cte using(competition_id)

left join clubs_cte home_clubs_cte
    on games_cte.home_club_id = home_clubs_cte.club_id

left join clubs_cte away_clubs_cte
    on games_cte.away_club_id = away_clubs_cte.club_id
