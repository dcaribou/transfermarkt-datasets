-- Curated clubs model.
-- Unions Transfermarkt and OpenFootball clubs via canonical mappings.
-- Transfermarkt is the primary source; OpenFootball-only clubs are appended.
-- Backward-compatible: all existing columns preserved, provenance columns added.

with tfmkt_clubs as (
    select
        bc.*,
        'transfermarkt' as source_system,
        bc.club_id as source_record_id,
        current_timestamp as ingested_at
    from {{ ref('base_clubs') }} bc
),

of_clubs as (
    select
        name as of_name,
        domestic_competition_id as of_competition_id
    from {{ ref('stg_openfootball_clubs') }}
),

club_map as (
    select
        canonical_club_id,
        source_club_name,
        confidence
    from {{ ref('map_club_ids') }}
    where source_system = 'openfootball'
),

-- OpenFootball clubs that are NOT matched to any Transfermarkt club
of_unmatched as (
    select
        cm.canonical_club_id as club_id,
        null as club_code,
        ofc.of_name as name,
        ofc.of_competition_id as domestic_competition_id,
        null::float as total_market_value,
        null::integer as squad_size,
        null::float as average_age,
        0 as foreigners_number,
        null::float as foreigners_percentage,
        null::integer as national_team_players,
        null as stadium_name,
        null::integer as stadium_seats,
        null as net_transfer_record,
        null as coach_name,
        null as last_season,
        null as filename,
        null as url,
        'openfootball' as source_system,
        ofc.of_name as source_record_id,
        current_timestamp as ingested_at
    from of_clubs ofc
    inner join club_map cm
        on cm.source_club_name = ofc.of_name
    where cm.confidence = 0.0
)

select * from tfmkt_clubs
union all
select * from of_unmatched

order by club_id
