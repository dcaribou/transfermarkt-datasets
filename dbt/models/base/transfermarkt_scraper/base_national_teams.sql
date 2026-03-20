with
    json_national_teams as (

        select
            str_split(filename, '/')[5] as season,
            filename as "filename",
            json(value) as json_row,
            str_split(json_extract_string(json_row, '$.href'), '/')[5] as national_team_id,
            row_number() over (partition by national_team_id order by season desc) as n

        from {{ source("transfermarkt_scraper", "national_teams") }}

    )

select
    national_team_id,
    json_extract_string(json_row, '$.name') as name,
    json_extract_string(json_row, '$.code') as team_code,
    json_extract_string(json_row, '$.parent.country_id') as country_id,
    case
        when json_extract_string(json_row, '$.confederation') = 'South American Football Confederation'
        then 'CONMEBOL'
        else json_extract_string(json_row, '$.confederation')
    end as confederation,
    json_extract_string(json_row, '$.team_image_url') as team_image_url,
    case
        when len(json_extract_string(json_row, '$.squad_size')) > 0
        then json_extract_string(json_row, '$.squad_size')::integer
        else null
    end as squad_size,
    case
        when len(json_extract_string(json_row, '$.average_age')) > 0
        then json_extract_string(json_row, '$.average_age')::float
        else null
    end as average_age,
    coalesce(
        case
            when json_extract_string(json_row, '$.foreigners_number') != 'null'
            then json_extract_string(json_row, '$.foreigners_number')::integer
            else null
        end,
        0
    ) as foreigners_number,
    case
        when
            json_extract_string(json_row, '$.foreigners_percentage') = 'null'
            or len(json_extract_string(json_row, '$.foreigners_percentage')) = 0
            or len(
                replace(
                    json_extract_string(json_row, '$.foreigners_percentage'), '%', ''
                )
            ) = 0
        then null
        else
            replace(
                json_extract_string(json_row, '$.foreigners_percentage'), '%', ''
            )::float
    end as foreigners_percentage,
    {{ parse_market_value("json_extract_string(json_row, '$.total_market_value')") }} as total_market_value,
    json_extract_string(json_row, '$.coach_name') as coach_name,
    case
        when len(json_extract_string(json_row, '$.fifa_ranking')) > 0
        then json_extract_string(json_row, '$.fifa_ranking')::integer
        else null
    end as fifa_ranking,
    season as last_season,
    "filename",
    'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href') as url

from json_national_teams

where n = 1
