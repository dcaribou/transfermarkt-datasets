# Fix #368: Incomplete 2024 game data for 6 newly added leagues

## Context

After adding 18 new leagues in PR #367, six first-tier leagues have incomplete 2024 game data:

| Competition | Name | Actual | Expected | % Complete |
|---|---|---|---|---|
| SA1 | Saudi Pro League | 213 | 306 | 69.6% |
| PL1 | Ekstraklasa | 218 | 306 | 71.2% |
| TS1 | Chance Liga | 176 | 240 | 73.3% |
| BRA1 | Série A | 297 | 380 | 78.2% |
| AUS1 | A-League Men | 129 | 156 | 82.7% |
| SER1 | Super liga Srbije | 220 | 240 | 91.7% |

The initial scrape only captured partial data. The `every_first_tier_league_has_games` dbt test currently has temporary exceptions for these 6 league/season pairs (added in commit `edf0045`).

The scraper connectivity issue (HTTP 405) appears to be resolved — transfermarkt.co.uk returns HTTP 200.

## Changes

### 1. Re-scrape games for season 2024

Run the scraper locally to re-acquire games for the 2024 season. The scraper fetches all competitions for the given season, and the `merge_output` function merges new records with existing ones (new records take precedence via deduplication by `href` key).

```sh
just acquire_local --asset games --seasons 2024
```

This updates `data/raw/transfermarkt-scraper/2024/games.json.gz` with the missing game records.

### 2. Re-scrape game_lineups for season 2024

Game lineups depend on games, so re-scrape these too to ensure the newly acquired games have lineup data:

```sh
just acquire_local --asset game_lineups --seasons 2024
```

### 3. Re-scrape appearances for season 2024

Appearances depend on players (which are already complete), but the new games will have appearances that were previously missing:

```sh
just acquire_local --asset appearances --seasons 2024
```

### 4. `dbt/tests/every_first_tier_league_has_games.sql`

Remove the 6 temporary exception entries for issue #368 (lines 61-67):

**Before:**
```sql
        -- Greece: 1 abandoned/cancelled match
        ('GR1',  '2014'),
        -- Incomplete initial scrape for newly added leagues (https://github.com/dcaribou/transfermarkt-datasets/issues/368)
        ('BRA1', '2024'),
        ('SA1',  '2024'),
        ('PL1',  '2024'),
        ('SER1', '2024'),
        ('TS1',  '2024'),
        ('AUS1', '2024')
    ) as t(competition_id, season)
```

**After:**
```sql
        -- Greece: 1 abandoned/cancelled match
        ('GR1',  '2014')
    ) as t(competition_id, season)
```

Remove the trailing comma after `('GR1', '2014')` and the 6 league entries plus their comment.

## Verification

### 1. Build the games model and run all tests

```sh
cd dbt && dbt deps && dbt build --target dev
```

**Expected:** All models build successfully. All tests pass, including `every_first_tier_league_has_games` (which no longer has exceptions for these 6 leagues).

### 2. Verify game counts for the 6 leagues

```sh
python -c "
import duckdb
conn = duckdb.connect('dbt/duck.db')
print(conn.sql(\"\"\"
    SELECT
        g.competition_id,
        c.name,
        g.season,
        count(distinct g.game_id) as actual_games,
        count(distinct g.home_club_id) as season_clubs,
        count(distinct g.home_club_id) * (count(distinct g.home_club_id) - 1) as expected_games
    FROM dev.games g
    JOIN dev.competitions c ON c.competition_id = g.competition_id
    WHERE g.competition_id IN ('SA1','PL1','TS1','BRA1','AUS1','SER1')
      AND g.season = '2024'
    GROUP BY 1, 2, 3
    ORDER BY g.competition_id
\"\"\").fetchdf().to_markdown(index=False))
"
```

**Expected:** For each league, `actual_games` equals `expected_games`.
