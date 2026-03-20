with
    json_countries as (

        select
            json(value) as json_row,
            json_extract_string(json_row, '$.country_id') as country_id,
            row_number() over (partition by country_id order by 1 desc) as n

        from {{ source("transfermarkt_scraper", "countries") }}

    )

select
    country_id,
    json_extract_string(json_row, '$.country_name') as country_name,
    json_extract_string(json_row, '$.country_code') as country_code,
    str_split(json_extract_string(json_row, '$.parent.href'), '/')[3] as confederation,
    case
        when len(json_extract_string(json_row, '$.total_clubs')) > 0
        then json_extract_string(json_row, '$.total_clubs')::integer
        else null
    end as total_clubs,
    case
        when len(json_extract_string(json_row, '$.total_players')) > 0
        then json_extract_string(json_row, '$.total_players')::integer
        else null
    end as total_players,
    case
        when len(json_extract_string(json_row, '$.average_age')) > 0
        then json_extract_string(json_row, '$.average_age')::float
        else null
    end as average_age,
    'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href') as url

from json_countries

where n = 1
