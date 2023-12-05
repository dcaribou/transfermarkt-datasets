with
    json_market_values as (

        select
            str_split(filename, '/')[5] as season,
            json(value) as json_row,
            json_extract_string(json_row, '$.player_id')::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n,
            filename
        
        from {{ source("transfermarkt_api", "market_value_development") }}
        

    ),
    unnested as (

        select
            unnest(json_transform(json_extract(json_row, '$.response.list'), '["JSON"]')) as json_row,
            player_id,
            season,
            filename

        from json_market_values

        where n = 1 -- take the last season extraction for each player

    ),
    -- remove duplicates in case multiple market values appear for the same player in the same day
    -- (it can happen)
    deduplicated as (

        select * from (

            select
                *,
                strptime(json_row ->> 'datum_mw', '%b %d, %Y') as "datetime",
                row_number() over (partition by player_id, "datetime"::date order by "datetime" desc) as n

            from unnested
            -- we need to remove the rows with no date information
            where (json_row ->> 'datum_mw')::varchar != '-'

        )
        where 
            n = 1 and
            -- since we are at it, let's remove the rows with no market value information as well
            (json_row ->> 'mw' != '-')
    )

select
    player_id,
    {{ parse_market_value("json_row ->> 'mw'") }} as market_value_in_eur,
    "datetime",
    ("datetime")::date as "date",
    date_trunc('week', "datetime") as dateweek,
    season as last_season,
    filename
    
from deduplicated
