{#
    For every completed season, each first-tier league should have exactly
    total_clubs * (total_clubs - 1) games (double round-robin), where
    total_clubs is derived from the actual number of clubs that played
    that season (not the static current-season value).
    The active season (max in the data) is excluded as it is still in progress.
    Only checks leagues with multiple seasons of data (to exclude newly-added
    leagues that may only have a partial scrape).
#}

with leagues_with_history as (
    select competition_id
    from {{ ref('games') }}
    group by 1
    having count(distinct season) > 1
),

-- Leagues with non-standard formats that consistently differ from double round-robin.
-- These are excluded entirely because their format is structurally different.
non_standard_format_leagues as (
    select competition_id from (values
        -- Scotland: plays 3 rounds (33 matchdays) then splits into top/bottom 6 for 5 more
        ('SC1'),
        -- Denmark: plays 3 rounds then splits into championship/relegation groups
        ('DK1'),
        -- Austria: splits into top/bottom 6 after regular season
        ('A1'),
        -- Switzerland: same split format as Austria
        ('C1'),
        -- Croatia: split format with playoff rounds
        ('KR1'),
        -- South Korea: split after 33 rounds into championship/relegation
        ('RSK1'),
        -- Argentina: apertura/clausura single round-robin format
        ('ARG1'),
        -- MLS: unbalanced conference-based schedule
        ('MLS1'),
        -- Mexico: clausura short tournament format
        ('MEX1'),
        -- Colombia: apertura/clausura format
        ('COL1'),
        -- Australia: regular season + finals series (extra playoff games)
        ('AUS1')
    ) as t(competition_id)
),

-- Specific season/competition pairs with known disruptions.
-- These are one-off exceptions, not structural format differences.
known_exceptions as (
    select competition_id, season from (values
        -- COVID-19: 2019/20 seasons curtailed or cancelled early
        ('FR1',  '2019'),  -- Ligue 1 cancelled with 10 rounds remaining
        ('NL1',  '2019'),  -- Eredivisie cancelled with 8 rounds remaining
        ('BE1',  '2019'),  -- Jupiler Pro League cancelled with 1 round remaining
        -- War in Ukraine: 2021/22 season suspended due to Russian invasion
        ('UKR1', '2021'),
        -- Ukraine league size changes and playoff formats
        ('UKR1', '2013'),  -- 16 teams but shortened season format
        ('UKR1', '2014'),  -- transition season after league restructuring
        ('UKR1', '2016'),  -- 12 teams with championship round (extra games)
        -- Greece: 1 abandoned/cancelled match
        ('GR1',  '2014')
    ) as t(competition_id, season)
),

league_season_games as (
    select
        g.competition_id,
        c.name,
        g.season,
        count(distinct g.game_id) as actual_games,
        count(distinct g.home_club_id) as season_clubs,
        count(distinct g.home_club_id) * (count(distinct g.home_club_id) - 1) as expected_games
    from {{ ref('games') }} g
    join {{ ref('competitions') }} c
        on c.competition_id = g.competition_id
    join leagues_with_history lh
        on lh.competition_id = g.competition_id
    where c.sub_type = 'first_tier'
      and g.season != (select max(season) from {{ ref('games') }})
      and g.competition_id not in (select competition_id from non_standard_format_leagues)
      and (g.competition_id, g.season) not in (select competition_id, season from known_exceptions)
    group by 1, 2, 3
)

select *
from league_season_games
where actual_games != expected_games
