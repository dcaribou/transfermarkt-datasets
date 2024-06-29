with
    json_game_lineups as (

        select
            json(value) as json_row,
            str_split(filename, '/')[5] as season,
            json_extract_string(json_row, '$.game_id') as game_id,
            json_extract_string(json_row, '$.home_club.formation') as home_club_formation,
            json_extract_string(json_row, '$.away_club.formation') as away_club_formation,
            row_number() over (partition by game_id order by season desc) as n

        from {{ source("transfermarkt_scraper", "game_lineups") }}

    ),
    json_raw_games as (

        select
            json(value) as json_row,
            str_split(filename, '/')[5] as season,
            (str_split(json_extract_string(json_row, '$.href'), '/')[5]) as game_id,
            row_number() over (partition by game_id order by season desc) as n

        from {{ source("transfermarkt_scraper", "games") }}

    ),
    json_games as (

        select
            json_raw_games.*,
            json_game_lineups.home_club_formation,
            json_game_lineups.away_club_formation

        from json_raw_games
        left join json_game_lineups 
            on json_raw_games.game_id = json_game_lineups.game_id and json_game_lineups.n = 1
        where json_raw_games.n = 1

    )

select
    game_id,
    (str_split(json_extract_string(json_row, '$.parent.href'), '/')[5]) as competition_id,
    season,
    json_extract_string(json_row, '$.matchday') as round,
    case
        when json_extract_string(json_row, '$.date') != 'null' then
        strptime(json_extract_string(json_row, '$.date'), '%a, %m/%d/%y')::date
        else null
    end as date,
    (str_split(json_extract_string(json_row, '$.home_club.href'), '/')[5])::integer as home_club_id,
    (str_split(json_extract_string(json_row, '$.away_club.href'), '/')[5])::integer
    as away_club_id,
    case
        when (json_row ->> 'result') != '-:-'
        then (str_split(json_row ->> 'result', ':')[1])::integer 
    end as home_club_goals,
    case
        when trim(json_row ->> 'result') != '-:-'
        then (str_split(json_row ->> 'result', ':')[2])::integer
    end as away_club_goals,
    case
        when
            (json_extract_string(json_row, '$.home_club_position') = 'null')
            or (json_extract_string(json_row, '$.home_club_position') = '')
        then -1
        else (str_split_regex(json_extract_string(json_row, '$.home_club_position'), '[\s]+'))[2]::integer
    end as home_club_position,
    case
        when
            (json_extract_string(json_row, '$.away_club_position') = 'null')
            or (json_extract_string(json_row, '$.away_club_position') = '')
        then -1
        else regexp_extract(json_extract_string(json_row, '$.away_club_position'), '\s([0-9]+)')::integer
    end as away_club_position,
    json_extract_string(json_row, '$.home_manager.name') as home_club_manager_name,
    json_extract_string(json_row, '$.away_manager.name') as away_club_manager_name,
    json_extract_string(json_row, '$.stadium') as stadium,
    replace(str_split(json_row ->> 'attendance', 'Attendance: ')[2], '.', '')::integer as attendance,
    json_extract_string(json_row, '$.referee') as referee,
    (
        'https://www.transfermarkt.co.uk' || json_extract_string(json_row, '$.href')
    ) as url,
    home_club_formation,
    away_club_formation

from json_games

where n = 1
and "date" is not null
