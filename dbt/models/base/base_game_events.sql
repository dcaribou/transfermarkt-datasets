with
    json_game_events as (

        select
            json(value) as json_row,
            str_split(filename, '/')[4] as season,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5]) as game_id
            
        from {{ source("raw_tfmkt", "games") }}

    ),
unnested as (

    select
        unnest(json_transform(json_extract(json_row, '$.events'), '["JSON"]')) as json_row,
        game_id
    from json_game_events

)

select
    game_id,
    (json_row -> 'minute')::integer as "minute",
    (json_row ->> 'type') as "type",
    str_split(json_row -> 'club' ->> 'href', '/')[5]::integer as club_id,
    str_split(json_row -> 'player' ->> 'href', '/')[5]::integer as player_id,
    (json_row -> 'action' ->> 'description') as description,
    str_split(json_row -> 'action' -> 'player_in' ->> 'href', '/')[5]::integer as player_in_id

from unnested

