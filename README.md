![Build Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/on-push.yml/badge.svg)
![Pipeline Status](https://github.com/dcaribou/transfermarkt-datasets/actions/workflows/on-schedule.yml/badge.svg)
![dbt Version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.6.2&color=orange)

# transfermarkt-datasets

In an nutshell, this project aims for three things:

1. Acquiring data from the transfermarkt website using the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper).
2. Building a **clean, public football (soccer) dataset** using data in 1.
3. Automating 1 and 2 to **keep assets up to date** and publicly available on some well-known data catalogs.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/dcaribou/transfermarkt-datasets/tree/master?quickstart=1)
[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://transfermarkt-datasets.fly.dev/)
[![Kaggle](https://kaggle.com/static/images/open-in-kaggle.svg)](https://www.kaggle.com/datasets/davidcariboo/player-scores)
[![data.world](https://img.shields.io/badge/-Open%20in%20data.world-blue?style=appveyor)](https://data.world/dcereijo/player-scores)

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

- [📥 setup](#-setup)
  - [make](#make)
- [💾 data storage](#-data-storage)
- [🕸️ data acquisition](#️-data-acquisition)
- [🔨 data preparation](#-data-preparation)
  - [python api](#python-api)
- [👁️ frontends](#️-frontends)
  - [🎈 streamlit](#-streamlit)
- [🏗️ infra](#️-infra)
- [💬 community](#-community)
  - [📞 getting in touch](#-getting-in-touch)
  - [🫶 sponsoring](#-sponsoring)
  - [👨‍💻 contributing](#-contributing)

------

## 📥 setup

> **🔈 New!** &rarr; Thanks to [Github codespaces](https://github.com/features/codespaces) you can now spin up a working dev environment in your browser with just a click, local setup required.
>
> [![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/dcaribou/transfermarkt-datasets/tree/master?quickstart=1)

Setup your local environment to run the project with `poetry`.
1. Install [poetry](https://python-poetry.org/docs/)
2. Install python dependencies (poetry will create a virtual environment for you)
```console
cd transfermarkt-datasets
poetry install
```
Remember to active the virtual environment once poetry has finished installing the dependencies by running `poetry shell`.

### make
The `Makefile` in the root defines a set of useful targets that will help you run the different parts of the project. Some examples are
```console
dvc_pull                       pull data from the cloud
docker_build                   build the project docker image and tag accordingly
acquire_local                  run the acquiring process locally (refreshes data/raw)
prepare_local                  run the prep process locally (refreshes data/prep)
sync                           run the sync process (refreshes data frontends)
streamlit_local                run streamlit app locally
dagit_local                    run dagit locally
```
Run `make help` to see the full list. Once you've completed the setup, you should be able to run most of these from your machine.

## 💾 data storage
All project data assets are kept inside the [`data`](data) folder. This is a [DVC](https://dvc.org/) repository, so all files can be pulled from the remote storage by running `make dvc_pull`.

path | description
-|-
`data/raw` | contains raw data per season as acquired with [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) (check [acquire](#-data-acquisition))
`data/prep` | contains prepared datasets as produced by dbt (check [prepare](#-data-preparation))

## 🕸️ data acquisition
In the scope of this project, "acquiring" is the process of collecting "raw data", as it is produced by [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper). Acquired data lives in the `data/raw` folder and it can be created or updated for a particular season by running `make acquire_local`

```console
make acquire_local ARGS="--asset all --season 2023"
```
This runs the scraper with a set of parameters and collects the output in `data/raw`.

## 🔨 data preparation
In the scope of this project, "preparing" is the process of transforming raw data to create a high quality dataset that can be conveniently consumed by analysts of all kinds.

Data prepartion is done in SQL using [dbt](https://docs.getdbt.com/) and [DuckDB](https://duckdb.org/). You can trigger a run of the preparation task using the `prepare_local` make target or work with the dbt CLI directly if you prefer.

* `cd dbt` &rarr; The [dbt](dbt) folder contains the dbt project for data preparation
* `dbt deps` &rarr; Install dbt packages. This is only required the first time you run dbt.
* `dbt run -m +appearances` &rarr; Refresh the appearances file by running the model in dbt.

dbt runs will populate a `dbt/duck.db` file in your local, which you can "connect to" using the DuckDB CLI and query the data using SQL.
```console
duckdb dbt/duck.db -c 'select * from dev.games'
```

![dbt](resources/dbt.png)

### python api
A thin python wrapper is provided as a convenience utility to help with loading and inspecting the dataset (for example, from a notebook).

```python
# import the module
from transfermarkt_datasets.core.dataset import Dataset

# instantiate the datasets handler
td = Dataset()

# load all assets into memory as pandas dataframes
td.load_assets()

# inspect assets
td.asset_names # ["games", "players", ...]
td.assets["games"].prep_df # get the built asset in a dataframe

# get raw data in a dataframe
td.assets["games"].load_raw()
td.assets["games"].raw_df 
```

The module code lives in the `transfermark_datasets` folder with the structure below.

path | description
-|-
`transfermark_datasets/core` | core classes and utils that are used to work with the dataset
`transfermark_datasets/tests` | unit tests for core classes
`transfermark_datasets/assets` | perpared asset definitions: one python file per asset

For more examples on using `transfermark_datasets`, checkout the sample [notebooks](notebooks).

## 👁️ frontends
Prepared data is published to a couple of popular dataset websites. This is done running `make sync`, which runs weekly as part of the [data pipeline](.github/workflows/on-schedule.yml).

* [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores)
* [data.world](https://data.world/dcereijo/player-scores)

### 🎈 streamlit
There is a [streamlit](https://streamlit.io/) app for the project with documentation, a data catalog and sample analyisis. The app is currently hosted in fly.io, you can check it out [here](https://transfermarkt-datasets.fly.dev/).

For local development, you can also run the app in your machine. Provided you've done the [setup](#-setup), run the following to spin up a local instance of the app
```console
make streamlit_local
```
> :warning: Note that the app expects prepared data to exist in `data/prep`. Check out [data storage](#-data-storage) for instructions about how to populate that folder.

## 🏗️ [infra](infra)
Define all the necessary infrastructure for the project in the cloud with Terraform.

## 💬 community

### 📞 getting in touch
In order to keep things tidy, there are two simple guidelines
* Keep the conversation centralised and public by getting in touch via the [Discussions](https://github.com/dcaribou/transfermarkt-datasets/discussions) tab.
* Avoid topic duplication by having a quick look at the [FAQs](https://github.com/dcaribou/transfermarkt-datasets/discussions/175)

### 🫶 sponsoring
Maintenance of this project is made possible by <a href="https://github.com/sponsors/dcaribou">sponsors</a>. If you'd like to sponsor this project you can use the `Sponsor` button at the top.

&rarr; I would like to express my grattitude to [@mortgad](https://github.com/mortgad) for becoming the first sponsor of this project.

### 👨‍💻 contributing
Contributions to `transfermarkt-datasets` are most welcome. If you want to contribute new fields or assets to this dataset, the instructions are quite simple:
1. [Fork the repo](https://github.com/dcaribou/transfermarkt-datasets/fork)
2. Set up your [local environment](#-setup)
3. [Populate `data/raw` directory](#-data-storage)
4. Start modifying assets or creating new ones in [the dbt project](#-data-preparation)
5. If it's all looking good, create a pull request with your changes :rocket:

> ℹ️ In case you face any issue following the instructions above please [get in touch](#-getting-in-touch)
