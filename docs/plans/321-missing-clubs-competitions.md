# Fix #321: Missing clubs in `clubs.csv` and discrepancy in `competitions.json`

## Context

Clubs from several European countries (Switzerland, Austria, Croatia, Poland, Czech Republic, etc.) are missing from `clubs.csv` because their domestic competitions are not listed in `data/competitions.json`. This file drives the scraper — only competitions listed there get their clubs/games/players scraped. The upstream scraper source (`transfermarkt-scraper/samples/competitions.json`) has 62 entries, but local has only 44.

## Changes

### 1. Add 20 missing competitions to `data/competitions.json`

Append the following competition entries (sourced from the upstream scraper file) to the end of the file, before the international competitions block:

| Code | Country | Type |
|------|---------|------|
| KR1 | Croatia | first_tier |
| A1 | Austria | first_tier |
| ofb | Austria | domestic_cup |
| ZYP1 | Cyprus | first_tier |
| CYSC | Cyprus | domestic_super_cup |
| NO1 | Norway | first_tier |
| NOPO | Norway | domestic_cup |
| UNG1 | Hungary | first_tier |
| SE1 | Sweden | first_tier |
| SEC | Sweden | domestic_cup |
| RO1 | Romania | first_tier |
| ROMS | Romania | domestic_super_cup |
| SER1 | Serbia | first_tier |
| PL1 | Poland | first_tier |
| POPU | Poland | domestic_cup |
| PLSC | Poland | domestic_super_cup |
| C1 | Switzerland | first_tier |
| SCC | Switzerland | domestic_cup |
| TS1 | Czech Republic | first_tier |
| TSP | Czech Republic | domestic_cup |

Result: 64 entries (44 existing + 20 new). Existing entries (including FAC and UCOL) are preserved.

### 2. Update `dbt/dbt_project.yml` — `competition_codes` variable

- Add the 20 new competition codes: `KR1`, `A1`, `ofb`, `ZYP1`, `CYSC`, `NO1`, `NOPO`, `UNG1`, `SE1`, `SEC`, `RO1`, `ROMS`, `SER1`, `PL1`, `POPU`, `PLSC`, `C1`, `SCC`, `TS1`, `TSP`
- Add `UCOL` (already in competitions.json but was missing from this list)
- Remove duplicate `BE1` entry

This variable is used by `base_appearances.sql` to filter appearances.

### 3. Update `dbt/models/curated/models.yml` — test thresholds

Widen the row count test bounds to accommodate additional data from the new competitions. These are upper-bound adjustments only (min stays the same) since we don't have the scraped data yet:

| Model | Current max | New max |
|-------|------------|---------|
| competitions | 45 | 70 |
| clubs | 480 | 700 |
| players | 38000 | 48000 |
| games | 82000 | 115000 |
| appearances | 2000000 | 2800000 |
| club_games | 176000 | 240000 |
| game_events | 1500000 | 2100000 |
| player_valuations | 550000 | 750000 |

## Files to modify

- `data/competitions.json` — add 20 competition entries
- `dbt/dbt_project.yml` — update `competition_codes` list
- `dbt/models/curated/models.yml` — widen row count thresholds

## Verification

After making these changes:
1. Run `cd dbt && ../.venv/bin/dbt build -s base_competitions competitions --target dev` to verify the competitions model builds with the new entries
2. Full verification requires re-running the scraper (`just acquire_local`) to fetch data for the new competitions, then `just prepare_local` to rebuild all models

## Note

These code changes alone won't populate the missing clubs — the scraper must be re-run against the updated `competitions.json` to actually fetch raw data for the new competitions. But this sets up the configuration so the next scraper run will include them.
