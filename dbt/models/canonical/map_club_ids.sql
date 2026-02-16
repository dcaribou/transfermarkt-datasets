-- Canonical club ID mapping across sources.
-- Maps source-native club identifiers to a canonical club_id.
-- Uses fuzzy name matching between Transfermarkt club names and OpenFootball team names.
-- For Phase 0, Transfermarkt club_id serves as canonical where a match exists.

with transfermarkt_clubs as (
    select
        club_id as tfmkt_club_id,
        name as tfmkt_name,
        domestic_competition_id as tfmkt_competition_id
    from {{ ref('base_clubs') }}
),

openfootball_clubs as (
    select
        name as of_name,
        domestic_competition_id as of_competition_id
    from {{ ref('stg_openfootball_clubs') }}
),

competition_map as (
    select
        source_competition_id,
        canonical_competition_id,
        source_system
    from {{ ref('map_competition_ids') }}
),

-- Join OpenFootball clubs to their canonical competition, then to Transfermarkt clubs
-- in the same competition for name matching.
of_with_canonical_comp as (
    select
        o.of_name,
        o.of_competition_id,
        cm.canonical_competition_id
    from openfootball_clubs o
    inner join competition_map cm
        on cm.source_competition_id = o.of_competition_id
        and cm.source_system = 'openfootball'
),

tfmkt_with_canonical_comp as (
    select
        t.tfmkt_club_id,
        t.tfmkt_name,
        t.tfmkt_competition_id,
        cm.canonical_competition_id
    from transfermarkt_clubs t
    inner join competition_map cm
        on cm.source_competition_id = t.tfmkt_competition_id
        and cm.source_system = 'transfermarkt'
),

-- Exact name match within the same canonical competition
exact_matches as (
    select
        t.tfmkt_club_id as canonical_club_id,
        o.of_name,
        o.of_competition_id,
        t.tfmkt_club_id,
        t.tfmkt_name,
        'exact_name' as resolution_method,
        1.0 as confidence
    from of_with_canonical_comp o
    inner join tfmkt_with_canonical_comp t
        on o.canonical_competition_id = t.canonical_competition_id
        and lower(o.of_name) = lower(t.tfmkt_name)
),

-- Substring match (OpenFootball name contains Transfermarkt name or vice versa)
substring_matches as (
    select
        t.tfmkt_club_id as canonical_club_id,
        o.of_name,
        o.of_competition_id,
        t.tfmkt_club_id,
        t.tfmkt_name,
        'substring' as resolution_method,
        0.7 as confidence
    from of_with_canonical_comp o
    inner join tfmkt_with_canonical_comp t
        on o.canonical_competition_id = t.canonical_competition_id
        and (
            lower(o.of_name) like '%' || lower(t.tfmkt_name) || '%'
            or lower(t.tfmkt_name) like '%' || lower(o.of_name) || '%'
        )
    where not exists (
        select 1 from exact_matches e
        where e.of_name = o.of_name
          and e.of_competition_id = o.of_competition_id
    )
),

-- Combine all match methods; unmatched OpenFootball clubs flagged
all_of_matches as (
    select * from exact_matches
    union all
    select * from substring_matches
),

-- Deduplicate: keep highest confidence match per OpenFootball club
ranked_of_matches as (
    select
        *,
        row_number() over (
            partition by of_name, of_competition_id
            order by confidence desc, resolution_method
        ) as rn
    from all_of_matches
),

-- Transfermarkt entries (always canonical)
tfmkt_entries as (
    select
        tfmkt_club_id as canonical_club_id,
        'transfermarkt' as source_system,
        tfmkt_club_id as source_club_id,
        tfmkt_name as source_club_name,
        'exact' as resolution_method,
        1.0 as confidence
    from transfermarkt_clubs
),

-- OpenFootball entries (matched or unmatched)
of_entries as (
    select
        coalesce(m.canonical_club_id, 'of_' || md5(o.of_name)) as canonical_club_id,
        'openfootball' as source_system,
        coalesce(m.canonical_club_id, 'of_' || md5(o.of_name)) as source_club_id,
        o.of_name as source_club_name,
        coalesce(m.resolution_method, 'unmatched') as resolution_method,
        coalesce(m.confidence, 0.0) as confidence
    from openfootball_clubs o
    left join ranked_of_matches m
        on m.of_name = o.of_name
        and m.of_competition_id = o.of_competition_id
        and m.rn = 1
)

select * from tfmkt_entries
union all
select * from of_entries
order by canonical_club_id, source_system
