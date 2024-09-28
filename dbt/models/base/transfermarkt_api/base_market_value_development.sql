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
                json_row ->> 'datum_mw' as datetime_str,
                strptime(datetime_str, '%b %d, %Y') as "datetime",
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
    datetime_str,
    -- we came across some dates in the future, which are clearly wrong
    -- so are we are truncating them to the current date
    case
        when ("datetime")::date > current_date then current_date
        else ("datetime")::date
    end as "date",
    season as last_season,
    filename
    
from deduplicated

-- this is a bug in the data
where datetime_str != 'May 15, 5023' 
