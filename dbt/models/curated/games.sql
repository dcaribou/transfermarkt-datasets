-- Curated games model.
-- Unions Transfermarkt and OpenFootball games via canonical mappings.
-- Transfermarkt is the primary source; OpenFootball-only games are appended.
-- Backward-compatible: all existing columns preserved, provenance columns added.

with games_cte as (
    select * from {{ ref('base_games') }}
),

competitions_cte as (
    select * from {{ ref('base_competitions') }}
),

clubs_cte as (
    select * from {{ ref('base_clubs') }}
),

event_club_names as (
    select distinct on(club_id)
        club_id,
        club_name
    from {{ ref('base_game_events') }}
    where club_name is not null
),

tfmkt_games as (
    select
        games_cte.*,
        coalesce(home_clubs_cte.name, home_event_names.club_name) as home_club_name,
        coalesce(away_clubs_cte.name, away_event_names.club_name) as away_club_name,
        games_cte.home_club_goals || ':' || games_cte.away_club_goals as "aggregate",
        competitions_cte.type as competition_type,
        'transfermarkt' as source_system,
        games_cte.game_id as source_record_id,
        current_timestamp as ingested_at
    from games_cte
    left join competitions_cte using(competition_id)
    left join clubs_cte home_clubs_cte
        on games_cte.home_club_id = home_clubs_cte.club_id
    left join clubs_cte away_clubs_cte
        on games_cte.away_club_id = away_clubs_cte.club_id
    left join event_club_names home_event_names
        on games_cte.home_club_id = home_event_names.club_id
    left join event_club_names away_event_names
        on games_cte.away_club_id = away_event_names.club_id
),

of_games as (
    select * from {{ ref('stg_openfootball_games') }}
),

game_map as (
    select
        canonical_game_id,
        source_game_id,
        confidence
    from {{ ref('map_match_ids') }}
    where source_system = 'openfootball'
),

-- OpenFootball games that are NOT matched to any Transfermarkt game
of_unmatched as (
    select
        og.game_id,
        og.competition_id,
        og.season,
        og.round,
        og.date,
        null::integer as home_club_id,
        null::integer as away_club_id,
        og.home_club_goals,
        og.away_club_goals,
        -1 as home_club_position,
        -1 as away_club_position,
        null as home_club_manager_name,
        null as away_club_manager_name,
        null as stadium,
        null::integer as attendance,
        null as referee,
        null as url,
        null as home_club_formation,
        null as away_club_formation,
        og.home_club_name,
        og.away_club_name,
        og.home_club_goals || ':' || og.away_club_goals as "aggregate",
        null as competition_type,
        'openfootball' as source_system,
        og.game_id as source_record_id,
        current_timestamp as ingested_at
    from of_games og
    inner join game_map gm
        on gm.source_game_id = og.game_id
    where gm.confidence = 0.0
)

select * from tfmkt_games
union all
select * from of_unmatched

order by game_id
