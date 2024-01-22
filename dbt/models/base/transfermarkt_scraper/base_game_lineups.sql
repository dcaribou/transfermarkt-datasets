with
    json_game_lineups as (

        select
            json(value) as raw_json_row,
            str_split(filename, '/')[5] as season,
            json_extract_string(raw_json_row, '$.game_id')::integer as game_id,
            row_number() over (partition by game_id order by season desc) as n

        from {{ source("transfermarkt_scraper", "game_lineups") }}
        
    ),
    home_club_starting_lineup as (

        select
            unnest(json_transform(json_extract(raw_json_row, '$.home_club.starting_lineup'), '["JSON"]')) as json_row,
            (str_split(json_extract_string(raw_json_row, '$.home_club.href'), '/')[5])::integer as club_id,
            'starting_lineup' as "type",
            game_id
        from json_game_lineups

        where n = 1

    ),
    home_club_substitutes as (

        select
            unnest(json_transform(json_extract(raw_json_row, '$.home_club.substitutes'), '["JSON"]')) as json_row,
            (str_split(json_extract_string(raw_json_row, '$.home_club.href'), '/')[5])::integer as club_id,
            'substitutes' as "type",
            game_id
        from json_game_lineups

        where n = 1

    ),
    away_club_starting_lineup as (

        select
            unnest(json_transform(json_extract(raw_json_row, '$.away_club.starting_lineup'), '["JSON"]')) as json_row,
            (str_split(json_extract_string(raw_json_row, '$.away_club.href'), '/')[5])::integer as club_id,
            'starting_lineup' as "type",
            game_id
        from json_game_lineups

        where n = 1

    ),
    away_club_substitutes as (

        select
            unnest(json_transform(json_extract(raw_json_row, '$.away_club.substitutes'), '["JSON"]')) as json_row,
            (str_split(json_extract_string(raw_json_row, '$.away_club.href'), '/')[5])::integer as club_id,
            'substitutes' as "type",
            game_id
        from json_game_lineups

        where n = 1

    ),
    all_game_lineups as (

        select * from home_club_starting_lineup
        union
        select * from home_club_substitutes
        union
        select * from away_club_starting_lineup 
        union
        select * from away_club_substitutes

    )

select
    game_id,
    club_id,
    "type",
    (json_row ->> 'number') as "number",
    (str_split((json_row ->> 'href'), '/')[5])::integer as player_id,
    (json_row ->> 'name') as "player_name",
    (json_row ->> 'team_captain')::integer as "team_captain",
    (json_row ->> 'position') as "position",

from all_game_lineups
