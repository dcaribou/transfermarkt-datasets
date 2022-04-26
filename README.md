# transfermarkt-datasets

- [transfermarkt-datasets](#transfermarkt-datasets)
  - [acquire](#acquire)
  - [prepare](#prepare)
    - [python api](#python-api)
    - [script](#script)
    - [infra](#infra)

In an nutshell, this project has three main efforts:

1. Acquire data from transfermarkt website using the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper).
2. Build a **clean, public football (soccer) dataset** using data in 1.
3. Automatate 1 and 2 to **keep these assets up to date** and publicly available on some well-known data catalogs.

:white_check_mark: [Kaggle](https://www.kaggle.com/davidcariboo/player-scores) :white_check_mark: [data.world](https://data.world/dcereijo/player-scores)

All project data assets are kept inside the `data` folder. This is a [DVC](https://dvc.org/) repository, therefore all files for the current revision can be pulled from remote storage with the `dvc pull` command.

> :information_source: Read access to the [DVC remote storage](https://dvc.org/doc/command-reference/remote#description) for this project is required to successfully run `dvc pull`. Contributors should feel free to grant themselves access by adding their AWS IAM user ARN to [this whitelist](https://github.com/dcaribou/transfermarkt-datasets/blob/6b6dd6572f582b2c40039913a65ba99d10fd1f44/infra/main.tf#L16). Have a look at [this PR](https://github.com/dcaribou/transfermarkt-datasets/pull/47/files) for an example.



![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/diagram.png?raw=true)

## acquire
In the scope of this project, "raw data" is that produced by [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper). It lives in the `data/raw` folder and it can be created or updated for a particular season using the `1_acquire.py` script.

```console
$ python 1_acquire.py local --asset all --season 2021
```

This dependency is the reason why [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) exists as a sub-module in this project. The `1_acquire.py` is nothing more than a helpper script that runs a `trasfermarkt-scraper` with a set of parameters and collects the results at the desired location.

## prepare
The main goal of this project is use the raw data that we have collected in 1 to create a high quality dataset that can be conveniently consumed by analysts of all kinds. The `transfermark_dataset` module provides a 

### python api

```python
# load data in 
td = TransfermarktDatasets(source_path="raw/data")
td.build_datasets()
td.asset_names # ["games", "players"...]
td.assets["games"].prep_df #

```

### script

Scripts for transforming scraped `raw` data into a cleaned, validated [data package](https://specs.frictionlessdata.io/) that can be used as the basis of further analysis in this project. You may run these scripts to produce the prepared dataset within `data/prep` using `2_prepare.py`.
```console
$ python 2_prepare.py local [--raw-files-location data/raw]
```
For reference on the types of assets produced by this script checkout published datasets linked above.

The preparation step uses `raw` data as input, hence raw files need to be available locally in order to run this step. You may pull raw assets by running `dvc pull` as mentioned earlier or by acquiring new and updated raw assets via `1_acquire.py` 

### [infra](infra)
Define all the necessary infrastructure for the project in the cloud with Terraform.

