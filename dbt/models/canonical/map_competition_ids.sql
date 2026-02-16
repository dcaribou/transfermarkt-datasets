-- Canonical competition ID mapping across sources.
-- Maps source-native competition identifiers to a canonical competition_id.
-- For Phase 0, Transfermarkt IDs serve as canonical where a match exists;
-- OpenFootball-only competitions get their own canonical ID prefixed with 'of_'.

with transfermarkt_competitions as (
    select
        competition_id as tfmkt_competition_id,
        competition_code as tfmkt_competition_code,
        name as tfmkt_name
    from {{ ref('base_competitions') }}
),

openfootball_competitions as (
    select
        competition_id as of_competition_id,
        name as of_name
    from {{ ref('stg_openfootball_competitions') }}
),

-- Known mappings between OpenFootball codes and Transfermarkt competition codes.
-- Maintained manually; each row links one OpenFootball competition code to
-- the corresponding Transfermarkt competition code.
-- Known mappings between OpenFootball codes and Transfermarkt competition IDs.
-- Only includes competitions that exist in the Transfermarkt dataset.
known_mappings (of_code, tfmkt_id) as (
    values
        ('en.1',    'GB1'),
        ('de.1',    'L1'),
        ('es.1',    'ES1'),
        ('it.1',    'IT1'),
        ('fr.1',    'FR1'),
        ('nl.1',    'NL1'),
        ('pt.1',    'PO1'),
        ('be.1',    'BE1'),
        ('tr.1',    'TR1'),
        ('gr.1',    'GR1'),
        ('sco.1',   'SC1'),
        ('ru.1',    'RU1'),
        ('uefa.cl', 'CL')
),

mapped as (
    select
        coalesce(t.tfmkt_competition_id, 'of_' || o.of_competition_id) as canonical_competition_id,
        'transfermarkt' as source_system,
        t.tfmkt_competition_id as source_competition_id,
        t.tfmkt_competition_code as source_competition_code,
        case when t.tfmkt_competition_id is not null then 'exact' else null end as resolution_method,
        case when t.tfmkt_competition_id is not null then 1.0 else null end as confidence
    from transfermarkt_competitions t
    left join known_mappings km on km.tfmkt_id = t.tfmkt_competition_id
    left join openfootball_competitions o on o.of_competition_id = km.of_code

    union all

    select
        coalesce(t.tfmkt_competition_id, 'of_' || o.of_competition_id) as canonical_competition_id,
        'openfootball' as source_system,
        o.of_competition_id as source_competition_id,
        o.of_competition_id as source_competition_code,
        case
            when t.tfmkt_competition_id is not null then 'exact'
            else 'unmatched'
        end as resolution_method,
        case
            when t.tfmkt_competition_id is not null then 1.0
            else 0.0
        end as confidence
    from openfootball_competitions o
    left join known_mappings km on km.of_code = o.of_competition_id
    left join transfermarkt_competitions t on t.tfmkt_competition_id = km.tfmkt_id
)

select * from mapped
order by canonical_competition_id, source_system
