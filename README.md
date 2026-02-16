![Build Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/build.yml/badge.svg)
![Scraper Pipeline Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/acquire-transfermarkt-scraper.yml/badge.svg)
![API Pipeline Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/acquire-transfermarkt-api.yml/badge.svg)
![dbt Version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.11.5&color=orange)

# transfermarkt-datasets

Clean, structured and **automatically updated** football (soccer) dataset built from [Transfermarkt](https://www.transfermarkt.co.uk/) data -- 68,000+ games, 30,000+ players, 1,500,000+ appearances and more, refreshed weekly.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/dcaribou/transfermarkt-datasets/tree/master?quickstart=1)
[![Kaggle](https://kaggle.com/static/images/open-in-kaggle.svg)](https://www.kaggle.com/datasets/davidcariboo/player-scores)
[![data.world](https://img.shields.io/badge/-Open%20in%20data.world-blue?style=appveyor)](https://data.world/dcereijo/player-scores)

## What's in it

The dataset is composed of **10 tables** covering competitions, games, clubs, players, appearances, player valuations, club games, game events, game lineups and transfers. Each table contains the attributes of the entity and IDs that can be used to join them together.

| Table | Description | Scale |
| --- | --- | --- |
| `competitions` | Leagues and tournaments | 40+ |
| `clubs` | Club details, squad size, market value | 400+ |
| `players` | Player profiles, positions, market values | 30,000+ |
| `games` | Match results, lineups, attendance | 68,000+ |
| `appearances` | One row per player per game played | 1,500,000+ |
| `player_valuations` | Historical market value records | 450,000+ |
| `club_games` | Per-club view of each game | 136,000+ |
| `game_events` | Goals, cards, substitutions | 950,000+ |
| `game_lineups` | Starting and bench lineups | 81,000+ |
| `transfers` | Player transfers between clubs | -- |

**Sample: `players`**

| player_id | name | current_club_name | position | market_value_in_eur | country_of_citizenship | date_of_birth |
| --- | --- | --- | --- | --- | --- | --- |
| 28003 | Lionel Messi | Inter Miami CF | Attack | 25000000 | Argentina | 1987-06-24 |
| 581678 | Florian Wirtz | Bayer 04 Leverkusen | Midfield | 200000000 | Germany | 2003-05-03 |
| 342229 | Kylian Mbappe | Real Madrid | Attack | 180000000 | France | 1998-12-20 |
| 418560 | Erling Haaland | Manchester City | Attack | 180000000 | Norway | 2000-07-21 |
| 401923 | Lamine Yamal | FC Barcelona | Attack | 150000000 | Spain | 2007-07-13 |

<details>
<summary><strong>ER diagram</strong></summary>

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

</details>

## Quick start

**Query directly with DuckDB** -- no download required:
```sql
-- pip install duckdb
INSTALL httpfs; LOAD httpfs;

SELECT player_id, name, position, market_value_in_eur
FROM read_csv_auto('https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/players.csv.gz')
WHERE position = 'Attack'
ORDER BY market_value_in_eur DESC
LIMIT 10;
```

All tables are available at `https://pub-e682421888d945d684bcae8890b0ec20.r2.dev/data/<table>.csv.gz`.

The data is also available on [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores) and [data.world](https://data.world/dcereijo/player-scores).

## Community

### Getting in touch
In order to keep things tidy, there are two simple guidelines
* Keep the conversation centralised and public by getting in touch via the [Discussions](https://github.com/dcaribou/transfermarkt-datasets/discussions) tab.
* Avoid topic duplication by having a quick look at the [FAQs](https://github.com/dcaribou/transfermarkt-datasets/discussions/175)

### Sponsoring
Maintenance of this project is made possible by <a href="https://github.com/sponsors/dcaribou">sponsors</a>. If you'd like to sponsor this project you can use the `Sponsor` button at the top.

&rarr; I would like to express my gratitude to [@mortgad](https://github.com/mortgad) for becoming the first sponsor of this project.

## Contributing
Contributions to `transfermarkt-datasets` are most welcome. If you want to contribute new fields or assets to this dataset, the instructions are quite simple:
1. [Fork the repo](https://github.com/dcaribou/transfermarkt-datasets/fork)
2. Set up your [local environment](#setup)
3. [Populate `data/raw` directory](#data-storage)
4. Start modifying assets or creating new ones in [the dbt project](#data-preparation)
5. If it's all looking good, create a pull request with your changes :rocket:

> In case you face any issue following the instructions above please [get in touch](#getting-in-touch)

<details>
<summary><strong>Developer guide</strong></summary>

### Setup

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

#### just
The `justfile` in the root defines a set of useful recipes that will help you run the different parts of the project. Some examples are
```console
dvc_pull                       pull data from the cloud
docker_build                   build the project docker image and tag accordingly
acquire_local                  run the acquiring process locally (refreshes data/raw/<acquirer>)
prepare_local                  run the prep process locally (refreshes data/prep)
sync                           run the sync process (refreshes data frontends)
streamlit_local                run streamlit app locally
```
Run `just --list` to see the full list. Once you've completed the setup, you should be able to run most of these from your machine.

### Data storage
All project data assets are kept inside the [`data`](data) folder. This is a [DVC](https://dvc.org/) repository, so all files can be pulled from remote storage by running `dvc pull`. Data is stored in [Cloudflare R2](https://developers.cloudflare.com/r2/) and served via a public URL, so no credentials are needed for pulling.

To **push** data to the remote, you need R2 credentials configured as per-remote DVC config:
```console
dvc remote modify --local r2 access_key_id <R2_ACCESS_KEY_ID>
dvc remote modify --local r2 secret_access_key <R2_SECRET_ACCESS_KEY>
```
This stores credentials in `.dvc/config.local` (gitignored) without conflicting with AWS credentials.

| path        | description                                                                                                                                                                     |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data/raw`  | contains raw data for [different acquirers](https://github.com/dcaribou/transfermarkt-datasets/discussions/202#discussioncomment-7142557) (check the data acquisition section below) |
| `data/prep` | contains prepared datasets as produced by dbt (check [data preparation](#data-preparation))                                                                                             |

### Data acquisition
In the scope of this project, "acquiring" is the process of collecting data from a specific source and via an acquiring script. Acquired data lives in the `data/raw` folder.

#### Acquirers
An acquirer is a script that collects data from somewhere and puts it in `data/raw`. They are defined in the [`scripts/acquiring`](scripts/acquiring) folder and run using the `acquire_local` recipe.
For example, to run the `transfermarkt-api` acquirer with a set of parameters, you can run
```console
just --set acquirer transfermarkt-api --set args "--season 2024" acquire_local
```
which will populate `data/raw/transfermarkt-api` with the data it collected. You can also run [the script](scripts/acquiring/transfermarkt-api.py) directly if you prefer.
```console
cd scripts/acquiring && python transfermarkt-api.py --season 2024
```

### Data preparation
In the scope of this project, "preparing" is the process of transforming raw data to create a high quality dataset that can be conveniently consumed by analysts of all kinds.

Data preparation is done in SQL using [dbt](https://docs.getdbt.com/) and [DuckDB](https://duckdb.org/). You can trigger a run of the preparation task using the `prepare_local` recipe or work with the dbt CLI directly if you prefer.

* `cd dbt` &rarr; The [dbt](dbt) folder contains the dbt project for data preparation
* `dbt deps` &rarr; Install dbt packages. This is only required the first time you run dbt.
* `dbt run -m +appearances` &rarr; Refresh the assets by running the corresponding model in dbt.

dbt runs will populate a `dbt/duck.db` file in your local, which you can "connect to" using the DuckDB CLI and query the data using SQL.
```console
duckdb dbt/duck.db -c 'select * from dev.games'
```

![dbt](resources/dbt.png)

> :warning: Make sure that you are using a DuckDB version that matches [that which is used in the project](.devcontainer/devcontainer.json).

### Frontends
Prepared data is published to a couple of popular dataset websites. This is done running `just sync`, which runs weekly as part of the [data pipeline](#orchestration).

* [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores)
* [data.world](https://data.world/dcereijo/player-scores)

There is a [streamlit](https://streamlit.io/) app for the project with documentation, a data catalog and sample analysis. For local development, run the following to spin up a local instance of the app:
```console
just streamlit_local
```
> :warning: Note that the app expects prepared data to exist in `data/prep`. Check out [data storage](#data-storage) for instructions about how to populate that folder.

### [Infra](infra)
Define all the necessary infrastructure for the project in the cloud with Terraform.

### Orchestration
The data pipeline is orchestrated as a series of Github Actions workflows. They are defined in the [`.github/workflows`](.github/workflows) folder and are triggered by different events.

| workflow name            | triggers on                                                  | description                                                                                                   |
| ------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `build`*                  | Every push to the `master` branch or to an open pull request | It runs the [data preparation](#data-preparation) step, and tests and commits a new version of the prepared data if there are any changes |
| `acquire-<acquirer>.yml` | Schedule                                                     | It runs the acquirer and commits the acquired data to the corresponding raw location                                                                      |
| `sync-<frontend>.yml`    | Every change on prepared data                                | It syncs the prepared data to the corresponding frontend                                                                                |

*`build-contribution` is the same as `build` but without committing any data.

> Debugging workflows remotely is a pain. I recommend using [act](https://github.com/nektos/act) to run them locally to the extent that is possible.

</details>
