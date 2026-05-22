# Fix #369: Build failing — partial-scrape root cause in CI workflow

## Context

The master build fails on 3 dbt tests. Two of those (SE1 2024 short-count and Aug–Nov 2025 sparse appearances) trace back to a single root cause in the CI acquisition workflow that silently turns partial scrapes into permanent data loss, even though the local acquisition script is deliberately upsert-only.

### The bug

[scripts/acquiring/transfermarkt-scraper.py:241-289](scripts/acquiring/transfermarkt-scraper.py) implements `merge_output()` as a proper upsert (UNION ALL → `ROW_NUMBER() PARTITION BY _key`, fresh wins). Locally this works correctly: re-acquiring SE1 2024 just now scraped 240 fresh games and merged into the existing file without losing the other leagues.

The CI workflow [.github/workflows/acquire-transfermarkt-scraper.yml](.github/workflows/acquire-transfermarkt-scraper.yml) breaks the upsert. Each parallel `acquire-clubs / acquire-players / acquire-games / acquire-appearances / acquire-game-lineups` job does `actions/checkout@v4` only — that restores the small `data/raw/transfermarkt-scraper.dvc` pointer file but **not** the actual `data/raw/transfermarkt-scraper/<season>/<asset>.json.gz` data files. There is **no `dvc pull` step** in those jobs (only in the final `dvc-push` job).

Consequence: when `just acquire_local --asset appearances --seasons 2025` runs in CI, the existing `appearances.json.gz` does not exist on disk. `merge_output()` enters the `existing_has_data=False` branch and writes the freshly-scraped data verbatim. If that scrape is partial (e.g., the run that produced today's 4,543-row file because tfmkt hit a mid-scrape error), the partial output is uploaded as the artifact. In `dvc-push`, the artifact is downloaded *after* `dvc pull` has restored the full prior data — so the partial artifact overwrites the full prior data, then `dvc commit -f` permanently records the regression.

This is also why the test exception list has been growing over time (SA1, PL1, TS1, BRA1, AUS1, SER1 in #368, now SE1 2024) — each new league experiences this issue once or twice on its way to a full scrape.

### Failure-by-failure status

1. **`every_first_tier_league_has_games`** — `SE1` 2024 had 214 raw games. After running `just acquire_local --asset games --seasons 2024 --competitions SE1` locally, the raw file now has 240 SE1 games. **Recoverable.**

2. **`expect_column_distinct_count_to_be_greater_than` on `appearances`** — months `2025-08` … `2025-11` show 359 / 440 / 428 / 449 distinct appearances; the 2025 raw appearance file has only 4,543 rows total vs ~403k in 2024. **Almost certainly recoverable** by a full local re-acquisition (the merge will rebuild the full set from prior local copies plus a fresh scrape). The CI fix below also prevents this from regressing again.

3. **`market_value_development_null_responses`** — season 2025 has 65.5% null responses in `data/raw/transfermarkt-api/2025/market_values.json`. **Deferred** — needs separate investigation. May be the same CI partial-scrape pattern affecting the API workflow, or a genuine Transfermarkt API issue. Do not change the test severity until we know which.

## Changes

### 1. Re-acquire SE1 2024 (already done locally during diagnosis)

```sh
source .venv/bin/activate
just acquire_local --asset games --seasons 2024 --competitions SE1
```

Result confirmed: SE1 2024 raw count is now 240 games. Need to also pull `appearances` and `game_lineups` for SE1 2024 once tfmkt's transient appearance error subsides:

```sh
just acquire_local --asset appearances --seasons 2024 --competitions SE1
just acquire_local --asset game_lineups --seasons 2024 --competitions SE1
```

(The appearance scrape failed once during diagnosis with `tfmkt failed with no output for appearances` — a transient `Connection refused` on a single player profile. Retry until clean.)

### 2. Re-acquire 2025 raw scraper data fully

Locally (where merge will preserve any existing rows):

```sh
just acquire_local --asset all --seasons 2025
```

Verify the resulting `data/raw/transfermarkt-scraper/2025/appearances.json.gz` has on the order of hundreds of thousands of rows, not 4,543.

### 3. Fix the CI workflow — pull only the asset each job needs

Edit [.github/workflows/acquire-transfermarkt-scraper.yml](.github/workflows/acquire-transfermarkt-scraper.yml). Insert a **targeted** `dvc pull` for the single asset file each job will merge into, **between `actions/checkout` and `run acquire`** in each of the five acquire jobs. DVC supports pulling a specific path inside a directory output, so we don't need to pull the whole 454 MB tree.

Per-job pulls (keyed off the existing `$DATA_DIR` env var which already resolves to `data/raw/transfermarkt-scraper/$SEASON`):

| job | path to pull |
|---|---|
| `acquire-clubs` | `${DATA_DIR}/clubs.json.gz` |
| `acquire-players` | `${DATA_DIR}/players.json.gz` |
| `acquire-games` | `${DATA_DIR}/games.json.gz` |
| `acquire-game-lineups` | `${DATA_DIR}/game_lineups.json.gz` |
| `acquire-appearances` | `${DATA_DIR}/appearances.json.gz` |

Concrete shape for each job:

```yaml
- uses: actions/checkout@v4
- name: pull existing raw data for this asset
  run: dvc pull "${DATA_DIR}/<asset>.json.gz"
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.R2_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.R2_SECRET_ACCESS_KEY }}
- uses: actions/download-artifact@v4
  ...
- name: run acquire
  ...
```

This ensures `merge_output()` in [scripts/acquiring/transfermarkt-scraper.py:241](scripts/acquiring/transfermarkt-scraper.py) finds the prior file and performs the intended upsert, while avoiding the cost of pulling the full directory in every job. The credentials and `dvc pull` invocation are already proven in the existing `dvc-push` job.

Apply the same per-asset pattern to [.github/workflows/acquire-transfermarkt-api.yml](.github/workflows/acquire-transfermarkt-api.yml) if it has the same shape — likely the cause of the market-value-null issue.

### 4. Commit the refreshed DVC pointer

After steps 1 and 2:

```sh
poetry run dvc commit data/raw/transfermarkt-scraper.dvc
```

Push happens when the user pushes the branch (the workflow handles `dvc push` separately on master).

### Not changing (yet)

- `dbt/tests/every_first_tier_league_has_games.sql` — re-acquisition already restores SE1 2024 to 240 games.
- `dbt/models/curated/models.yml` — re-acquisition expected to lift 2025 appearance counts above the 9,500 floor.
- `dbt/tests/market_value_development_null_responses.sql` — deferred until we know whether the 65.5% null rate is from the same CI bug or genuine API behavior.

## Verification

After re-acquisition + workflow fix, rebuild dbt locally:

```sh
cd dbt && ../.venv/bin/dbt build --target dev
```

Then check the three previously failing tests:

```sh
cd dbt && ../.venv/bin/dbt test \
  --select every_first_tier_league_has_games \
           market_value_development_null_responses \
           appearances \
  --target dev
```

Expected:
- `every_first_tier_league_has_games` → PASS
- `appearances` distinct-count expectation → PASS
- `market_value_development_null_responses` → still ERROR for now (deferred)

Spot-check raw counts:

```sh
.venv/bin/python -c "import duckdb; print(duckdb.connect('dbt/duck.db', read_only=True).sql(\"select competition_id, season, count(distinct game_id) as games from dev.games where competition_id = 'SE1' group by 1,2 order by season\").fetchdf().to_markdown(index=False))"
```

Expected: `SE1` 2024 = 240 games.

```sh
.venv/bin/python -c "import duckdb; print(duckdb.connect('dbt/duck.db', read_only=True).sql(\"select date_trunc('month', \\\"date\\\") as month, count(distinct appearance_id) as apps from dev.appearances where \\\"date\\\" >= '2025-08-01' and \\\"date\\\" < '2025-12-01' group by month order by month\").fetchdf().to_markdown(index=False))"
```

Expected: each month comfortably above 9,500 distinct appearances.

To validate the CI workflow change without waiting for the next scheduled run, dispatch the workflow manually with `gh workflow run acquire-transfermarkt-scraper.yml -f season=2025` and inspect the resulting commit's diff to `data/raw/transfermarkt-scraper.dvc` — the directory size should *grow*, not stay flat or shrink.

## Critical files

- [.github/workflows/acquire-transfermarkt-scraper.yml](.github/workflows/acquire-transfermarkt-scraper.yml) — add `dvc pull` to each acquire job
- [.github/workflows/acquire-transfermarkt-api.yml](.github/workflows/acquire-transfermarkt-api.yml) — likely needs the same fix; inspect first
- [data/raw/transfermarkt-scraper.dvc](data/raw/transfermarkt-scraper.dvc) — gets refreshed by `dvc commit` after local re-acquisition
- [scripts/acquiring/transfermarkt-scraper.py](scripts/acquiring/transfermarkt-scraper.py) — reference: `merge_output()` at lines 241-289 is correct; the bug is environmental, not in this code
