with
    json_market_values as (

        select
            str_split(filename, '/')[5] as season,
            json(value) as json_row,
            json_extract_string(json_row, '$.player_id')::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n
        
        from {{ source("transfermarkt_api", "market_value_development") }}
        

    )

select
    player_id,
    {{ parse_market_value("json_extract_string(json_row, '$.response.current')") }} as market_value_in_eur,
    {{ parse_market_value("json_extract_string(json_row, '$.response.highest')") }} as highest_market_value_in_eur,
    json_row

from json_market_values

where n = 1
