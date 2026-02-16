-- Extract distinct competitions from OpenFootball match files.
-- Each JSON file corresponds to one competition + season.

with source as (
    select
        name as competition_name,
        filename,
        replace(str_split(filename, '/')[-1], '.json', '') as competition_code,
        str_split(filename, '/')[-2]::integer as season
    from {{ source('openfootball', 'matches') }}
),

deduplicated as (
    select
        competition_code,
        competition_name,
        row_number() over (
            partition by competition_code
            order by season desc
        ) as n
    from source
)

select
    competition_code as competition_id,
    competition_code,
    competition_name as name
from deduplicated
where n = 1
order by competition_code
