with games_cte as (

    select * from {{ ref('base_games') }}

),
competitions_cte as (

    select * from {{ ref('base_competitions') }}

),
clubs_cte as (

    select * from {{ ref('base_clubs') }}

),
event_club_names as (

    select distinct on(club_id)
        club_id,
        club_name
    from {{ ref('base_game_events') }}
    where club_name is not null

)

select
    games_cte.*,
    coalesce(home_clubs_cte.name, home_event_names.club_name) as home_club_name,
    coalesce(away_clubs_cte.name, away_event_names.club_name) as away_club_name,
    games_cte.home_club_goals || ':' || games_cte.away_club_goals as "aggregate",
    competitions_cte.type as competition_type

from games_cte

left join competitions_cte using(competition_id)

left join clubs_cte home_clubs_cte
    on games_cte.home_club_id = home_clubs_cte.club_id

left join clubs_cte away_clubs_cte
    on games_cte.away_club_id = away_clubs_cte.club_id

left join event_club_names home_event_names
    on games_cte.home_club_id = home_event_names.club_id

left join event_club_names away_event_names
    on games_cte.away_club_id = away_event_names.club_id

order by games_cte.game_id
