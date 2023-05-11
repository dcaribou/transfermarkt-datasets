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
    competitions_cte.type as competition_type

from games_cte

left join competitions_cte using(competition_id)
