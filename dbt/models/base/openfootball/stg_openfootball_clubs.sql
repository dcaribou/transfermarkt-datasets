-- Extract distinct clubs from OpenFootball match data.
-- Club identity is derived from team names across all matches.

with source as (
    select
        filename,
        replace(str_split(filename, '/')[-1], '.json', '') as competition_code,
        unnest(matches) as match
    from {{ source('openfootball', 'matches') }}
),

all_clubs as (
    select
        match.team1 as club_name,
        competition_code
    from source
    union
    select
        match.team2 as club_name,
        competition_code
    from source
),

deduplicated as (
    select
        club_name,
        competition_code,
        row_number() over (
            partition by club_name
            order by competition_code
        ) as n
    from all_clubs
)

select
    club_name as name,
    competition_code as domestic_competition_id
from deduplicated
where n = 1
order by club_name
