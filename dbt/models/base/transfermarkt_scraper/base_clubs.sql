with
    json_players as (

        select
            str_split(filename, '/')[5] as season,
            filename as "filename",
            json(value) as json_row,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5]) as club_id,
            row_number() over (partition by club_id order by season desc) as n

        from {{ source("transfermarkt_scraper", "clubs") }}

    )

select
    club_id,
    json_extract_string(json_row, '$.code') as club_code,
    json_extract_string(json_row, '$.name') as "name",
    (
        str_split(json_extract_string(json_row, '$.parent.href'), '/')[5]
    ) as domestic_competition_id,
    case
        when json_row -> 'total_market_value' is not null
        then (json_row ->> '$.total_market_value')::float
        else null
    end as total_market_value,
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
            )
            = 0
        then null
        else
            replace(
                json_extract_string(json_row, '$.foreigners_percentage'), '%', ''
            )::float
    end as foreigners_percentage,
    json_extract_string(json_row, '$.national_team_players')::integer
    as national_team_players,
    json_extract_string(json_row, '$.stadium_name') as stadium_name,
    replace(
        str_split(json_extract_string(json_row, '$.stadium_seats'), ' ')[1], '.', ''
    )::integer as stadium_seats,
    json_extract_string(json_row, '$.net_transfer_record') as net_transfer_record,
    json_extract_string(json_row, '$.coach_name') as coach_name,
    season as last_season,
    "filename",
    (
        'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href')
    ) as url

from json_players

where n = 1
