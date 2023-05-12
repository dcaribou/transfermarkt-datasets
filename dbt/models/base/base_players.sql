with
    json_players as (

        select
            str_split(filename, '/')[4] as season,
            json(value) as json_row,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5])::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n
        
        from {{ source("raw_tfmkt", "players") }}
        

    )

select
    case when json_row ->> 'current_market_value' = '-' then null
    else json_row ->> 'current_market_value'
    end as current_market_value_in_eur,
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
    json_extract_string(json_row, '$.place_of_birth.country') as country_of_birth,
    json_extract_string(json_row, '$.place_of_birth.city') as city_of_birth,
    json_extract_string(json_row, '$.citizenship') as country_of_citizenship,

    strptime(
        case
            when json_extract_string(json_row, '$.date_of_birth') not in ('N/A', 'null')
            then json_extract_string(json_row, '$.date_of_birth')
            else null
        end,
        '%b %d, %Y'
    )::date as date_of_birth,
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
    then trim(regexp_replace((json_row ->> 'height')[:4], '[,\s]', ''))::integer
    else null
    end as height_in_cm,
    json_row ->> 'current_market_value',
    {#
    prep_df["market_value_in_eur"] = (
      json_normalized["current_market_value"].apply(parse_market_value)
    )
    prep_df["highest_market_value_in_eur"] = (
      json_normalized["highest_market_value"].apply(parse_market_value)
    )
    #}
    json_extract_string(json_row, '$.player_agent.name') as agent_name,
    strptime(
        case when json_extract_string(json_row, '$.contract_expires') = '-' then null
        else
            case
                when json_extract_string(json_row, '$.contract_expires') not similar to '^[0-9]{2}'
                then '1 ' || json_extract_string(json_row, '$.contract_expires')
                else json_extract_string(json_row, '$.contract_expires')
            end
        end,
        '%b %-d, %Y'
    )::date as contract_expiration_date,
    json_extract_string(json_row, '$.image_url') as image_url,
    'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href') as url

from json_players

where n = 1
and current_market_value_in_eur is not null
