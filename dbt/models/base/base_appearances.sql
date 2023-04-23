with
    json_appearances as (

        select json(value) as json_row from {{ source("raw_tfmkt", "appearances") }}

    )
select

    (str_split(json_extract_string(json_row, '$.href'), '/')[5])::integer as player_id,
    coalesce(
        (str_split(json_extract_string(json_row, '$.result.href'), '/')[5])::integer, -1
    ) as game_id,
    (player_id || '_' || game_id) as appearance_id,
    json_extract_string(json_row, '$.competition_code') as competition_id,
    (str_split(json_extract_string(json_row, '$.for.href'), '/')[5])::integer
    as player_club_id,
    case
        when len(json_extract_string(json_row, '$.goals')) = 0
        then 0
        else json_extract_string(json_row, '$.goals')::integer
    end as goals,
    case
        when len(json_extract_string(json_row, '$.assists')) = 0
        then 0
        else json_extract_string(json_row, '$.assists')::integer
    end as assists,
    case
        when len(json_extract_string(json_row, '$.minutes_played')) = 0
        then 0
        else (json_extract_string(json_row, '$.minutes_played')[:-1])::integer
    end as minutes_played,
    case
        when
            (
                len(json_extract_string(json_row, '$.yellow_cards')) > 0
                or len(json_extract_string(json_row, '$.second_yellow_cards')) > 0
            )
        then 1
        else 0
    end as yellow_cards,
    case
        when len(json_extract_string(json_row, '$.red_cards')) > 0 then 1 else 0
    end as red_cards

from json_appearances
