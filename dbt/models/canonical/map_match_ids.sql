-- Canonical match/game ID mapping across sources.
-- Matches games between sources using date + canonical club IDs.
-- For Phase 0, Transfermarkt game_id serves as canonical where a match exists.

with transfermarkt_games as (
    select
        game_id as tfmkt_game_id,
        competition_id as tfmkt_competition_id,
        season,
        date,
        home_club_id::varchar as tfmkt_home_club_id,
        away_club_id::varchar as tfmkt_away_club_id
    from {{ ref('base_games') }}
    where date is not null
),

openfootball_games as (
    select
        game_id as of_game_id,
        competition_id as of_competition_id,
        season,
        date,
        home_club_name as of_home_name,
        away_club_name as of_away_name
    from {{ ref('stg_openfootball_games') }}
),

club_map as (
    select
        canonical_club_id,
        source_system,
        source_club_id,
        source_club_name,
        confidence as club_confidence
    from {{ ref('map_club_ids') }}
),

competition_map as (
    select
        canonical_competition_id,
        source_system,
        source_competition_id
    from {{ ref('map_competition_ids') }}
),

-- Resolve OpenFootball club names to canonical club IDs
of_resolved as (
    select
        og.of_game_id,
        og.of_competition_id,
        og.season,
        og.date,
        og.of_home_name,
        og.of_away_name,
        hcm.canonical_club_id as canonical_home_club_id,
        acm.canonical_club_id as canonical_away_club_id,
        comp_map.canonical_competition_id
    from openfootball_games og
    left join club_map hcm
        on hcm.source_club_name = og.of_home_name
        and hcm.source_system = 'openfootball'
    left join club_map acm
        on acm.source_club_name = og.of_away_name
        and acm.source_system = 'openfootball'
    left join competition_map comp_map
        on comp_map.source_competition_id = og.of_competition_id
        and comp_map.source_system = 'openfootball'
),

-- Match by date + canonical home + canonical away club
matched as (
    select
        tg.tfmkt_game_id,
        ofr.of_game_id,
        tg.tfmkt_game_id as canonical_game_id,
        'date_clubs' as resolution_method,
        case
            when ofr.canonical_home_club_id is not null
             and ofr.canonical_away_club_id is not null
            then 1.0
            else 0.5
        end as confidence
    from of_resolved ofr
    inner join transfermarkt_games tg
        on tg.date = ofr.date
        and tg.tfmkt_home_club_id = ofr.canonical_home_club_id
        and tg.tfmkt_away_club_id = ofr.canonical_away_club_id
),

-- Transfermarkt entries (always canonical)
tfmkt_entries as (
    select
        tfmkt_game_id as canonical_game_id,
        'transfermarkt' as source_system,
        tfmkt_game_id as source_game_id,
        'exact' as resolution_method,
        1.0 as confidence
    from transfermarkt_games
),

-- OpenFootball entries (matched or unmatched)
of_entries as (
    select
        coalesce(m.canonical_game_id, ofr.of_game_id) as canonical_game_id,
        'openfootball' as source_system,
        ofr.of_game_id as source_game_id,
        coalesce(m.resolution_method, 'unmatched') as resolution_method,
        coalesce(m.confidence, 0.0) as confidence
    from of_resolved ofr
    left join matched m on m.of_game_id = ofr.of_game_id
)

select * from tfmkt_entries
union all
select * from of_entries
order by canonical_game_id, source_system
