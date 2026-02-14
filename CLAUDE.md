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

DuckDB is already installed in the virtualenv (pulled as a dbt dependency), no separate install needed.

After a dbt build, a `dbt/duck.db` database is created. Query it with:

```sh
.venv/bin/duckdb dbt/duck.db -c 'SELECT * FROM dev.games LIMIT 10'
```

To query the prepared CSV files directly (no build required):

```sh
.venv/bin/duckdb -c "SELECT * FROM read_csv_auto('data/prep/games.csv.gz') LIMIT 10"
```

## Raw data format

Transfermarkt uses **European date format** (DD/MM/YY). When parsing dates in base models, try `%d/%m/%y` before `%m/%d/%y` to avoid day/month swaps on ambiguous dates.

## Tests

```sh
just test                     # Python unit tests (pytest)
cd dbt && ../.venv/bin/dbt test --target dev  # dbt data tests
```
