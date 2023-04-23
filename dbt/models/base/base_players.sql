with
    json_players as (

        select json(value) as json_row from {{ source("raw_tfmkt", "players") }}

    )

select

    (str_split(json_extract_string(json_row, '$.href'), '/')[5])::integer as player_id,
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
    json_extract_string(json_row, '$.last_season')::integer as season,
    coalesce(
        str_split(json_extract_string(json_row, '$.parent.href'), '/')[5], -1
    ) as current_club_id,

    {# prep_df["player_code"] = self.url_unquote(href_parts[1]) #}
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
        when
            json_extract_string(json_row, '$.position') in (
                'Centre-Forward',
                'Left Winger',
                'Right Winger',
                'Second Striker',
                'Attack'
            )
        then 'Attack'
        when
            json_extract_string(json_row, '$.position')
            in ('Centre-Back', 'Left-Back', 'Right-Back', 'Defender')
        then 'Defender'
        when
            json_extract_string(json_row, '$.position') in (
                'Attacking Midfield',
                'Central Midfield',
                'Defensive Midfield',
                'Left Midfield',
                'Right Midfield',
                'Midfield'
            )
        then 'Midfield'
        when json_extract_string(json_row, '$.position') = 'Goalkeeper'
        then 'Goalkeeper'
        else 'Missing'
    end as position,
    str_split(json_extract_string(json_row, '$.position'), ' - ')[2] as sub_position,

    case
        when json_extract_string(json_row, '$.foot') = 'N/A'
        then null
        else json_extract_string(json_row, '$.foot')
    end as foot,

    case
        when json_extract_string(json_row, '$.height') = 'N/A'
        then null
        else
            (
                ((str_split(json_extract_string(json_row, '$.height'), ' ')[0])::float)
                * 100
            )::integer
    end as height_in_cm,

    {#
    prep_df["market_value_in_eur"] = (
      json_normalized["current_market_value"].apply(parse_market_value)
    )
    prep_df["highest_market_value_in_eur"] = (
      json_normalized["highest_market_value"].apply(parse_market_value)
    )
    #}
    json_extract_string(json_row, '$.player_agent.name') as agent_name,
    {# strptime(
        case when json_extract_string(json_row, '$.contract_expires') = '-' then null
        else json_extract_string(json_row, '$.contract_expires') end,
        '%b [%-d], %Y'
    )::date as contract_expiration_date #}
    json_extract_string(json_row, '$.image_url') as image_url,
    'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href') as url

from json_players
