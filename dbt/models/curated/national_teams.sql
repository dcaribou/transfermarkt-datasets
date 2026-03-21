with national_teams_cte as (

    select * from {{ ref('base_national_teams') }}

),

countries_cte as (

    select * from {{ ref('base_countries') }}

)

select
    nt.national_team_id,
    nt.name,
    nt.team_code,
    nt.country_id,
    c.country_name,
    c.country_code,
    nt.confederation,
    nt.team_image_url,
    nt.squad_size,
    nt.average_age,
    nt.foreigners_number,
    nt.foreigners_percentage,
    nt.total_market_value,
    nt.coach_name,
    nt.fifa_ranking,
    nt.last_season,
    nt.url

from national_teams_cte nt
left join countries_cte c on nt.country_id = c.country_id

order by nt.national_team_id
