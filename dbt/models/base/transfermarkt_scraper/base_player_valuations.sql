with
    json_players as (

        select
            str_split(filename, '/')[5] as season,
            json(value) as json_row,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5])::integer as player_id,
            row_number() over (partition by player_id order by season desc) as n
        
        from {{ source("transfermarkt_scraper", "players") }}        

    ),
    deduped_seasons as (

        select * from json_players where n = 1

    ),
    unnested as (

    select
        unnest(json_transform(json_extract(json_row, '$.market_value_history'), '["JSON"]')) as json_row,
        player_id,
        season
    from deduped_seasons

    ),
    extracted as (
        
        select
            player_id,
            season as last_season,
            strptime(json_row ->> 'datum_mw', '%b %d, %Y') as "datetime",
            ("datetime")::date as "date",
            date_trunc('week', "datetime") as dateweek,
            {{ parse_market_value("json_row ->> 'mw'") }} as market_value_in_eur,
            row_number() over (partition by player_id, "date" order by "date" desc) as n

        from unnested
        where json_row ->> 'mw' != '-'

    )

select *
from extracted
where n = 1
and date_part('year', "date") != 5023
