-- Curated competitions model.
-- Unions Transfermarkt and OpenFootball competitions via canonical mappings.
-- Transfermarkt is the primary source; OpenFootball-only competitions are appended.
-- Backward-compatible: all existing columns preserved, provenance columns added.

with tfmkt_competitions as (
    select
        bc.*,
        case when bc.competition_id in (
            'ES1', 'GB1', 'IT1', 'FR1', 'L1'
        ) then true
        else false
        end as is_major_national_league,
        'transfermarkt' as source_system,
        bc.competition_id as source_record_id,
        current_timestamp as ingested_at
    from {{ ref('base_competitions') }} bc
),

of_competitions as (
    select
        oc.competition_id,
        oc.competition_code,
        oc.name
    from {{ ref('stg_openfootball_competitions') }} oc
),

competition_map as (
    select
        canonical_competition_id,
        source_system,
        source_competition_id,
        confidence
    from {{ ref('map_competition_ids') }}
    where source_system = 'openfootball'
),

-- OpenFootball competitions that are NOT matched to any Transfermarkt competition
of_unmatched as (
    select
        'of_' || ofc.competition_id as competition_id,
        ofc.competition_code,
        ofc.name,
        null as sub_type,
        'other' as "type",
        -1 as country_id,
        null as country_name,
        null as domestic_league_code,
        null as confederation,
        null as url,
        false as is_major_national_league,
        'openfootball' as source_system,
        ofc.competition_id as source_record_id,
        current_timestamp as ingested_at
    from of_competitions ofc
    inner join competition_map cm
        on cm.source_competition_id = ofc.competition_id
    where cm.confidence = 0.0
)

select * from tfmkt_competitions
union all
select * from of_unmatched

order by competition_id
