# CLAUDE.md

## Project overview

Football (soccer) dataset built from transfermarkt.com data. Raw data is acquired via scrapers/APIs into `data/raw/`, then transformed with dbt + DuckDB into prepared CSV files in `data/prep/`.

## Key paths

- `data/raw/transfermarkt-scraper/` - raw JSON data from the scraper
- `data/raw/transfermarkt-api/` - raw JSON data from the API
- `data/prep/` - prepared dataset CSV files (gzipped)
- `dbt/` - dbt project root (models, profiles, packages)
- `dbt/models/base/` - base models that parse raw JSON into tables (one per source)
- `dbt/models/curated/` - curated models that join/transform base models into final datasets

## Running dbt

dbt and all Python dependencies live in the local virtualenv:

```sh
.venv/bin/dbt <command>       # run dbt directly
just prepare_local            # full build (dbt deps + dbt build)
```

Always `cd dbt` before running dbt commands, or use `just prepare_local` from the repo root.

Build specific models:
```sh
cd dbt && ../.venv/bin/dbt build -s base_games games --target dev
```

## Querying data with DuckDB

DuckDB is available as a Python package in the virtualenv (no CLI binary). Use Python to run queries:

After a dbt build, a `dbt/duck.db` database is created. Query it with:

```sh
.venv/bin/python -c "import duckdb; print(duckdb.connect('dbt/duck.db').sql('SELECT * FROM dev.games LIMIT 10').fetchdf().to_markdown(index=False))"
```

To query the prepared CSV files directly (no build required):

```sh
.venv/bin/python -c "import duckdb; print(duckdb.sql(\"SELECT * FROM read_csv_auto('data/prep/games.csv.gz') LIMIT 10\").fetchdf().to_markdown(index=False))"
```

## Partial acquisition

The acquisition pipeline supports filters to scrape a subset of data without running the full pipeline. Filters are mutually exclusive — use only one at a time.

```sh
# Scrape all assets for a single competition (Premier League)
just acquire_local --asset all --seasons 2025 --competitions GB1

# Scrape multiple competitions
just acquire_local --asset all --seasons 2025 --competitions GB1,ES1,IT1

# Scrape only players for a specific club
just acquire_local --asset players --seasons 2025 --clubs 131

# Scrape appearances for a specific player
just acquire_local --asset appearances --seasons 2025 --players 28003

# Fetch API data (market values + transfers) for specific players
just --set acquirer transfermarkt-api acquire_local --seasons 2025 --players 28003,1122196
```

When using `--asset all` with a filter, only relevant downstream assets are acquired:
- `--competitions`: clubs, players, appearances, games, game_lineups
- `--clubs`: players, appearances
- `--players`: appearances

Partial scrapes merge cleanly with existing data via the DuckDB merge step.

## Raw data format

Transfermarkt uses **European date format** (DD/MM/YY). When parsing dates in base models, try `%d/%m/%y` before `%m/%d/%y` to avoid day/month swaps on ambiguous dates.

## Tests

```sh
just test                     # Python unit tests (pytest)
cd dbt && ../.venv/bin/dbt test --target dev  # dbt data tests
```
