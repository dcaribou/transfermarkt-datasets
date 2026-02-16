# Field-Level Provenance Specification

## Purpose
For curated fields that may come from multiple sources, this document defines the source precedence and derivation rules. A machine-readable YAML artifact (`dbt/field_provenance.yml`) is generated alongside the build and versioned in-repo.

## Provenance Model

Each entry in the provenance artifact describes one curated field:

```yaml
- model: <curated model name>
  field: <column name>
  sources:
    - system: <source_system>
      field: <source field path>
      priority: <integer, lower = higher priority>
  derivation: <rule description>
```

### Priority Rules
- **Priority 1** (primary): Transfermarkt -- the established, higher-coverage source.
- **Priority 2** (secondary): OpenFootball -- used when Transfermarkt does not provide the field or record.

### Derivation Types
- `direct`: Field maps 1:1 from a single source field.
- `coalesce`: Field is resolved by trying sources in priority order.
- `computed`: Field is derived from a combination of source fields.
- `static`: Field has a fixed value (e.g., source_system).

## Curated Model Provenance

### competitions

| Field | Sources | Derivation |
|---|---|---|
| competition_id | tfmkt: base_competitions.competition_id | direct (tfmkt) or prefixed (openfootball) |
| competition_code | tfmkt: base_competitions.competition_code, of: stg_openfootball_competitions.competition_code | coalesce |
| name | tfmkt: base_competitions.name, of: stg_openfootball_competitions.name | coalesce |
| sub_type | tfmkt: base_competitions.sub_type | direct |
| type | tfmkt: base_competitions.type | direct (default 'other' for openfootball) |
| country_id | tfmkt: base_competitions.country_id | direct |
| country_name | tfmkt: base_competitions.country_name | direct |
| domestic_league_code | tfmkt: base_competitions.domestic_league_code | direct |
| confederation | tfmkt: base_competitions.confederation | direct |
| url | tfmkt: base_competitions.url | direct |
| is_major_national_league | computed from competition_id | computed |
| source_system | row-level provenance | static |
| source_record_id | row-level provenance | static |
| ingested_at | row-level provenance | static |

### clubs

| Field | Sources | Derivation |
|---|---|---|
| club_id | tfmkt: base_clubs.club_id | direct (tfmkt) or canonical hash (openfootball) |
| club_code | tfmkt: base_clubs.club_code | direct |
| name | tfmkt: base_clubs.name, of: stg_openfootball_clubs.name | coalesce |
| domestic_competition_id | tfmkt: base_clubs.domestic_competition_id, of: stg_openfootball_clubs.domestic_competition_id | coalesce |
| total_market_value | tfmkt: base_clubs.total_market_value | direct |
| squad_size | tfmkt: base_clubs.squad_size | direct |
| average_age | tfmkt: base_clubs.average_age | direct |
| foreigners_number | tfmkt: base_clubs.foreigners_number | direct |
| foreigners_percentage | tfmkt: base_clubs.foreigners_percentage | direct |
| national_team_players | tfmkt: base_clubs.national_team_players | direct |
| stadium_name | tfmkt: base_clubs.stadium_name | direct |
| stadium_seats | tfmkt: base_clubs.stadium_seats | direct |
| net_transfer_record | tfmkt: base_clubs.net_transfer_record | direct |
| coach_name | tfmkt: base_clubs.coach_name | direct |
| url | tfmkt: base_clubs.url | direct |
| source_system | row-level provenance | static |
| source_record_id | row-level provenance | static |
| ingested_at | row-level provenance | static |

### games

| Field | Sources | Derivation |
|---|---|---|
| game_id | tfmkt: base_games.game_id, of: stg_openfootball_games.game_id | coalesce |
| competition_id | tfmkt: base_games.competition_id, of: stg_openfootball_games.competition_id | coalesce |
| season | tfmkt: base_games.season, of: stg_openfootball_games.season | coalesce |
| round | tfmkt: base_games.round, of: stg_openfootball_games.round | coalesce |
| date | tfmkt: base_games.date, of: stg_openfootball_games.date | coalesce |
| home_club_id | tfmkt: base_games.home_club_id | direct |
| away_club_id | tfmkt: base_games.away_club_id | direct |
| home_club_goals | tfmkt: base_games.home_club_goals, of: stg_openfootball_games.home_club_goals | coalesce |
| away_club_goals | tfmkt: base_games.away_club_goals, of: stg_openfootball_games.away_club_goals | coalesce |
| home_club_name | tfmkt: coalesce(base_clubs.name, base_game_events.club_name), of: stg_openfootball_games.home_club_name | coalesce |
| away_club_name | tfmkt: coalesce(base_clubs.name, base_game_events.club_name), of: stg_openfootball_games.away_club_name | coalesce |
| aggregate | computed: home_club_goals \|\| ':' \|\| away_club_goals | computed |
| competition_type | tfmkt: base_competitions.type | direct |
| home_club_position | tfmkt: base_games.home_club_position | direct |
| away_club_position | tfmkt: base_games.away_club_position | direct |
| home_club_manager_name | tfmkt: base_games.home_club_manager_name | direct |
| away_club_manager_name | tfmkt: base_games.away_club_manager_name | direct |
| home_club_formation | tfmkt: base_games.home_club_formation | direct |
| away_club_formation | tfmkt: base_games.away_club_formation | direct |
| stadium | tfmkt: base_games.stadium | direct |
| attendance | tfmkt: base_games.attendance | direct |
| referee | tfmkt: base_games.referee | direct |
| url | tfmkt: base_games.url | direct |
| source_system | row-level provenance | static |
| source_record_id | row-level provenance | static |
| ingested_at | row-level provenance | static |

## Versioning
This document and the corresponding YAML artifact are versioned in the repository. Any change to source precedence or derivation logic must update both this document and the artifact.
