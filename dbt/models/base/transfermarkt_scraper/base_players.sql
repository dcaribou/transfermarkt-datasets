with
    json_players as (

        select
            str_split(filename, '/')[5] as season,
            json(value) as json_row,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5])::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n
        
        from {{ source("transfermarkt_scraper", "players") }}
        

    )

select
    player_id,
    case
        when len(trim(json_extract_string(json_row, '$.name'))) = 0
        then null
        else trim(json_extract_string(json_row, '$.name'))
    end as first_name,
    case
        when len(trim(json_extract_string(json_row, '$.last_name'))) = 0
        then null
        else trim(json_extract_string(json_row, '$.last_name'))
    end as last_name,
    trim(coalesce(first_name, '') || ' ' || coalesce(last_name, '')) as name,
    season as last_season,
    coalesce(
        str_split(json_extract_string(json_row, '$.parent.href'), '/')[5], -1
    ) as current_club_id,
    json_extract_string(json_row, '$.code') as player_code,
    trim(json_extract_string(json_row, '$.place_of_birth.country')) as country_of_birth,
    trim(json_extract_string(json_row, '$.place_of_birth.city')) as city_of_birth,
    trim(json_extract_string(json_row, '$.citizenship')) as country_of_citizenship,

    case
        when json_extract_string(json_row, '$.date_of_birth') not in ('N/A', 'null', '')
        then 
            case
                when length(json_extract_string(json_row, '$.date_of_birth')) = 4
                then cast(json_extract_string(json_row, '$.date_of_birth') || '-01-01' as date)  -- Assume first day of the year if only year is given
                else strptime(json_extract_string(json_row, '$.date_of_birth'), '%b %d, %Y')  -- Handles full date like "Jan 01, 2000"
            end
        else null
    end as date_of_birth,
    
    case
        when json_extract_string(json_row, '$.position') = 'Goalkeeper' then 'Goalkeeper'
        else str_split(json_extract_string(json_row, '$.position'), ' - ')[2]
    end as sub_position,
    case
        when
            sub_position in (
                'Centre-Forward',
                'Left Winger',
                'Right Winger',
                'Second Striker',
                'Attack'
            )
        then 'Attack'
        when
            sub_position
            in ('Centre-Back', 'Left-Back', 'Right-Back', 'Defender')
        then 'Defender'
        when
            sub_position in (
                'Attacking Midfield',
                'Central Midfield',
                'Defensive Midfield',
                'Left Midfield',
                'Right Midfield',
                'Midfield'
            )
        then 'Midfield'
        when sub_position = 'Goalkeeper'
        then 'Goalkeeper'
        else 'Missing'
    end as position,
    case
        when json_extract_string(json_row, '$.foot') in ('N/A', 'null')
        then null
        else json_extract_string(json_row, '$.foot')
    end as foot,
    case when json_row ->> 'height' != 'null'
    then trim(regexp_replace((json_row ->> 'height')[:4], '[,\s´]', ''))::integer
    else null
    end as height_in_cm,
    {{ parse_contract_expiration_date("json_row ->> 'contract_expires'")}} as contract_expiration_date,
    json_row -> 'player_agent' ->> 'name' as agent_name,
    json_extract_string(json_row, '$.image_url') as image_url,
    'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href') as url

from json_players

where n = 1
