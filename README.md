![Build Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/build.yml/badge.svg)
![Scraper Pipeline Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/acquire-transfermarkt-scraper.yml/badge.svg)
![API Pipeline Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/acquire-transfermarkt-api.yml/badge.svg)
![dbt Version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.11.5&color=orange)

# transfermarkt-datasets

A **canonical open football (soccer) dataset** built from multiple sources. The project standardizes data from Transfermarkt, OpenFootball, and other sources into a single coherent schema with full source lineage and provenance tracking.

> **Vision:** Become the most trusted open dataset for professional football analysis by evolving from a Transfermarkt-first pipeline into a multi-source, canonical data platform. See [docs/vision.md](docs/vision.md) for the full roadmap.

### What this project does

1. **Acquires** raw data from multiple sources (Transfermarkt scraper/API, OpenFootball, and more to come).
2. **Standardizes** data through canonical mapping and staging layers using dbt + DuckDB.
3. **Publishes** a clean, versioned dataset with source lineage on popular data platforms.
4. **Automates** acquisition, transformation, quality checks, and publishing via CI/CD.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/dcaribou/transfermarkt-datasets/tree/master?quickstart=1)
[![Kaggle](https://kaggle.com/static/images/open-in-kaggle.svg)](https://www.kaggle.com/datasets/davidcariboo/player-scores)
[![data.world](https://img.shields.io/badge/-Open%20in%20data.world-blue?style=appveyor)](https://data.world/dcereijo/player-scores)

------

## Architecture

```
                   Sources                     Staging              Canonical        Curated
              +-----------------+         +---------------+     +-------------+   +----------+
              | Transfermarkt   |-------->| base_*        |---->|             |   |          |
              | (scraper + API) |         | models        |     | map_*_ids   |-->| competi- |
              +-----------------+         +---------------+     | (canonical  |   | tions    |
                                                                |  mappings)  |   | clubs    |
              +-----------------+         +---------------+     |             |   | games    |
              | OpenFootball    |-------->| stg_openfb_*  |---->|             |   | ...      |
              | (football.json) |         | models        |     +-------------+   +----------+
              +-----------------+         +---------------+           |               |
                                                                     v               v
                                                              Source lineage   source_system
                                                              & provenance     source_record_id
                                                              metadata         ingested_at
```

### Source lineage

Every curated row includes provenance columns:

| Column | Description |
|---|---|
| `source_system` | Which source produced this row (`transfermarkt` or `openfootball`) |
| `source_record_id` | The source-native identifier for traceability |
| `ingested_at` | Timestamp of when the row was ingested |

Field-level provenance metadata is documented in [docs/field-level-provenance.md](docs/field-level-provenance.md) and the machine-readable artifact at [dbt/field_provenance.yml](dbt/field_provenance.yml).

------
```mermaid
classDiagram
direction LR
competitions --|> games : competition_id
competitions --|> clubs : domestic_competition_id
clubs --|> players : current_club_id
clubs --|> club_games : opponent/club_id
clubs --|> game_events : club_id
players --|> appearances : player_id
players --|> game_events : player_id
players --|> player_valuations : player_id
games --|> appearances : game_id
games --|> game_events : game_id
games --|> clubs : home/away_club_id
games --|> club_games : game_id
class competitions {
 competition_id
}
class games {
    game_id
    home/away_club_id
    competition_id
}
class game_events {
    game_id
    player_id
}
class clubs {
    club_id
    domestic_competition_id
}
class club_games {
    club_id
    opponent_club_id
    game_id
}
class players {
    player_id
    current_club_id
}
class player_valuations{
    player_id
}
class appearances {
    appearance_id
    player_id
    game_id
}
```
------

- [transfermarkt-datasets](#transfermarkt-datasets)
  - [Setup](#setup)
  - [Data storage](#data-storage)
  - [Data acquisition](#data-acquisition)
  - [Data preparation](#data-preparation)
  - [Quality and profiling](#quality-and-profiling)
  - [Frontends](#frontends)
  - [Orchestration](#orchestration)
  - [Community](#community)

------

## Setup

> Thanks to [Github codespaces](https://github.com/features/codespaces) you can spin up a working dev environment in your browser with just a click, **no local setup required**.
>
> [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/dcaribou/transfermarkt-datasets/tree/master?quickstart=1)

Setup your local environment to run the project with `poetry`.
1. Install [poetry](https://python-poetry.org/docs/)
2. Install python dependencies (poetry will create a virtual environment for you)
```console
cd transfermarkt-datasets
poetry install
```
Remember to activate the virtual environment once poetry has finished installing the dependencies by running `poetry shell`.

### just
The `justfile` in the root defines a set of useful recipes that will help you run the different parts of the project. Some examples are
```console
dvc_pull                       pull data from the cloud
docker_build                   build the project docker image and tag accordingly
acquire_local                  run the acquiring process locally (refreshes data/raw/<acquirer>)
acquire_openfootball           run the openfootball acquirer locally
prepare_local                  run the prep process locally (refreshes data/prep)
profile                        run data profiling and drift checks
sync                           run the sync process (refreshes data frontends)
streamlit_local                run streamlit app locally
```
Run `just --list` to see the full list. Once you've completed the setup, you should be able to run most of these from your machine.

## Data storage
All project data assets are kept inside the [`data`](data) folder. This is a [DVC](https://dvc.org/) repository, so all files can be pulled from remote storage by running `dvc pull`. Data is stored in [Cloudflare R2](https://developers.cloudflare.com/r2/) and served via a public URL, so no credentials are needed for pulling.

Raw data follows the source-agnostic convention: `data/raw/<source>/<season>/<asset>.<ext>`.

| path        | description |
| ----------- | ----------- |
| `data/raw/transfermarkt-scraper/` | Raw data from the Transfermarkt scraper |
| `data/raw/transfermarkt-api/` | Raw data from the Transfermarkt API |
| `data/raw/openfootball/` | Raw data from OpenFootball (football.json) |
| `data/prep` | Prepared datasets as produced by dbt |

## Data acquisition
"Acquiring" is the process of collecting data from a specific source via an acquiring script. Acquired data lives in the `data/raw` folder.

### Acquirers
An acquirer is a script that collects data from somewhere and puts it in `data/raw/<source>/`. They are defined in the [`scripts/acquiring`](scripts/acquiring) folder.

| Acquirer | Source | Command |
|---|---|---|
| `transfermarkt-scraper` | Transfermarkt website | `just acquire_local` |
| `transfermarkt-api` | Transfermarkt REST API | `just --set acquirer transfermarkt-api acquire_local` |
| `openfootball` | OpenFootball GitHub repo | `just acquire_openfootball` |

## Data preparation
"Preparing" is the process of transforming raw data into a high quality dataset. The transformation pipeline has three layers:

1. **Base/staging models** (`dbt/models/base/`): Parse source-specific raw data into normalized tables.
2. **Canonical mapping models** (`dbt/models/canonical/`): Reconcile entity IDs across sources (competitions, clubs, matches).
3. **Curated models** (`dbt/models/curated/`): Join and enrich data into final consumer-facing tables with provenance columns.

Data preparation is done in SQL using [dbt](https://docs.getdbt.com/) and [DuckDB](https://duckdb.org/).

```console
cd dbt && dbt deps && dbt build --target dev
```

![dbt](resources/dbt.png)

## Quality and profiling
Automated quality checks run at multiple levels:

- **dbt tests**: Schema tests (uniqueness, not-null, accepted values, row count ranges) on every model.
- **Provenance validation**: CI checks that `source_system`, `source_record_id`, and `ingested_at` are present and populated.
- **Dynamic profiling**: `scripts/profiling/profile_models.py` computes row counts, null ratios, cardinality, and value ranges per source/model, and flags drift against stored baselines.

```console
just profile              # run drift checks against baselines
just profile_update_baseline  # update baselines from current build
```

## Frontends
Prepared data is published to a couple of popular dataset websites. This is done running `just sync`, which runs weekly as part of the data pipeline.

* [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores)
* [data.world](https://data.world/dcereijo/player-scores)

## Orchestration
The data pipeline is orchestrated as a series of Github Actions workflows defined in [`.github/workflows`](.github/workflows).

| workflow name | triggers on | description |
| --- | --- | --- |
| `build` | Push to `master` or open PR | Runs data preparation, tests, provenance validation, and profiling checks. Commits prepared data if changed. |
| `acquire-transfermarkt-scraper` | Schedule (Tue/Fri) | Scrapes Transfermarkt and commits raw data via DVC |
| `acquire-transfermarkt-api` | After scraper completes | Collects API data (market values, transfers) |
| `acquire-openfootball` | Schedule (Monday) / Manual | Downloads OpenFootball match data |
| `sync-<frontend>` | On prepared data changes | Syncs to Kaggle / data.world |

## Community

### Getting in touch
* Keep the conversation centralised and public via [Discussions](https://github.com/dcaribou/transfermarkt-datasets/discussions).
* Check the [FAQs](https://github.com/dcaribou/transfermarkt-datasets/discussions/175) before posting.

### Sponsoring
Maintenance of this project is made possible by [sponsors](https://github.com/sponsors/dcaribou). If you'd like to sponsor this project you can use the `Sponsor` button at the top.

### Contributing
Contributions are most welcome. To contribute:
1. [Fork the repo](https://github.com/dcaribou/transfermarkt-datasets/fork)
2. Set up your [local environment](#setup)
3. [Populate `data/raw`](#data-storage)
4. Start modifying or creating models in [the dbt project](#data-preparation)
5. Create a pull request with your changes
