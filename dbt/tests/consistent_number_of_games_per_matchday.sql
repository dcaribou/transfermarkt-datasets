{#
    Sanity check to see if there are any leagues with less than 9 games in a given matchday.
    https://github.com/dcaribou/transfermarkt-datasets/issues/283
#}

with games_cte as (

    select * from {{ ref('games') }}
),
competitions_cte as (

    select * from {{ ref('competitions') }}
)

select
 "round",
 season,
 competition_id,
 count(*)

from games_cte
left join competitions_cte using(competition_id)

where competitions_cte.is_major_national_league
and season != 2024

group by 1,2,3
having count(*) < 9
