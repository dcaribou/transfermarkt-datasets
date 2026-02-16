-- Staging model for OpenFootball match data.
-- Unnests the matches array from each competition JSON file.

with source as (
    select
        name as competition_name,
        filename,
        replace(str_split(filename, '/')[-1], '.json', '') as competition_code,
        str_split(filename, '/')[-2]::integer as season,
        unnest(matches) as match
    from {{ source('openfootball', 'matches') }}
)

select
    -- generate a deterministic game id from competition + season + date + teams
    md5(
        competition_code || '|' ||
        season::varchar || '|' ||
        match.date::varchar || '|' ||
        match.team1 || '|' ||
        match.team2
    ) as game_id,
    competition_code as competition_id,
    season::varchar as season,
    match.round as round,
    match.date::date as date,
    match.team1 as home_club_name,
    match.team2 as away_club_name,
    match.score.ft[1]::integer as home_club_goals,
    match.score.ft[2]::integer as away_club_goals,
    match.score.ht[1]::integer as home_club_goals_ht,
    match.score.ht[2]::integer as away_club_goals_ht
from source
where match.date is not null
order by competition_code, season, date
