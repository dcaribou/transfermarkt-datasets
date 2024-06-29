
with
    json_competitions as (

        select
            json(value) as json_row,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5]) as competition_id,
            row_number() over (partition by competition_id order by 1 desc) as n

        from {{ source("transfermarkt_scraper", "competitions") }}

    )

select
    competition_id,
    str_split((json_row ->> 'href'), '/')[2] as competition_code,
    competition_code as name,
    json_row ->> 'competition_type' as sub_type,
    case
        when sub_type = 'first_tier' then 'domestic_league'
        when sub_type = 'domestic_cup' then 'domestic_cup'
        when sub_type in (
            'uefa_champions_league', 'europa_league', 'uefa_europa_conference_league_qualifiers',
            'uefa_champions_league_qualifying', 'europa_league_qualifying', 'uefa_europa_conference_league',
            'uefa_super_cup'
        ) then 'international_cup'
        else 'other'
    end as "type",
    coalesce((json_row ->> 'country_id')::integer, -1) as country_id,
    (json_row ->> 'country_name') as country_name,
    (json_row ->> 'country_code') as domestic_league_code,
    str_split((json_row -> 'parent' ->> 'href'), '/')[3] as confederation,
    'https://www.transfermarkt.co.uk' || (json_row ->> 'href') as url,

from json_competitions

where n = 1
