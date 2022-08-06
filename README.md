# transfermarkt-datasets

In an nutshell, this project aims for three things:

1. Acquire data from transfermarkt website using the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper).
2. Build a **clean, public football (soccer) dataset** using data in 1.
3. Automatate 1 and 2 to **keep these assets up to date** and publicly available on some well-known data catalogs.

Checkout this dataset also in: :white_check_mark: [Kaggle](https://www.kaggle.com/davidcariboo/player-scores) | :white_check_mark: [data.world](https://data.world/dcereijo/player-scores)

| ![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/diagram.png?raw=true) | 
|:--:| 
| *High level data model for transfermarkt-datasets* |

TODO: change toc format
- [data storage](#data-storage)
- [data acquisition](#data-acquisition)
- [data preparation](#data-preparation)
  - [dagster](#dagster)
  - [configuration](#configuration)
  - [python api](#python-api)
- [infra](#infra)
- [contributing](#contributing)


## data storage
This is a [DVC](https://dvc.org/) repository, therefore all files for the current revision can be pulled from remote storage with the `dvc pull` command. All project data assets are kept inside the `data` folder.

* `data/raw`: contains raw data per season as acquired with [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) (check [acquire](#acquire))
* `data/prep`: contains the prepared datasets as produced by `transfermarkt_datasets` module (check [prepare](#prepare))

> :information_source: Read access to the [DVC remote storage](https://dvc.org/doc/command-reference/remote#description) for the project is required to successfully run `dvc pull`. Contributors should feel free to grant themselves access by adding their AWS IAM user ARN to [this whitelist](https://github.com/dcaribou/transfermarkt-datasets/blob/6b6dd6572f582b2c40039913a65ba99d10fd1f44/infra/main.tf#L16). Have a look at [this PR](https://github.com/dcaribou/transfermarkt-datasets/pull/47/files) for an example.

## data acquisition
In the scope of this project, "acquiring" is the process of collecting "raw data", as it is produced by [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper). Acquired data lives in the `data/raw` folder and it can be created or updated for a particular season using the `1_acquire.py` script.

```console
$ python 1_acquire.py local --asset all --season 2021
```

This dependency is the reason why [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) exists as a sub-module in this project. The `1_acquire.py` is a helper script that runs the scraper with a set of parameters and collects the output in `data/raw`.

## data preparation
In the scope of this project, "preparing" is the process of tranforming raw data to create a high quality dataset that can be conveniently consumed by analysts of all kinds. The `transfermark_datasets` module contains the preparation logic, which can be executed using the `2_prepare.py` script.

### dagster
The dataset preparation steps can be rendered as a dagster job and run either in dagit or with dagster command.

### configuration
Configuration is defined in the [config.yml](config.yml) file. The `assets` section references classes in [`transfermarkt_datasets/assets`](transfermarkt_datasets/assets), which define the logic for building and validating the different assets. 

### python api
`transfermark_datasets` provides a python api that can be used to work with the module from python rather than using the script. This is particularly convenient for working with the datasets from a notebook.
```python
# import the module
from transfermarkt_datasets.core.dataset import Dataset

# instantiate the datasets handler
td = Dataset()

# build the datasets from raw data
td.discover_assets()
td.build_datasets()

# inspect the results
td.asset_names # ["games", "players"...]
td.assets["games"].prep_df # get the built asset in a dataframe
td.assets["games"].load_raw_from_stage() # get the raw data in a dataframe
td.assets["games"].raw_df # get raw data in a dataframe
```
For more examples on using `transfermark_datasets`, checkout the sample [notebooks](notebooks).

## [infra](infra)
Define all the necessary infrastructure for the project in the cloud with Terraform.

## contributing
Contributions to `transfermarkt-datasets` are most welcome. If you want to contribute new fields or assets to this dataset, instructions are quite simple:
1. [Fork the repo](https://github.com/dcaribou/transfermarkt-datasets/fork) (make sure to initialize sub-modules as well with `git submodule update --init --recursive`)
2. Set up a new conda environment with `conda env create -f environment.yml`
3. Pull the raw data by either running `dvc pull` ([requesting access is needed](#dvc)) or using the `1_acquire.py` script (no access request is needed)
4. Start modifying assets or creating a new one in `transfermarkt_datasets/assets`. You can use `2_prepare.py` to run and test your changes.
5. If it's all looking good, create a pull request with your changes :rocket:
