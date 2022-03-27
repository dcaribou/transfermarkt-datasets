# transfermarkt-datasets
Use data from [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to **build a clean, public football (soccer) dataset**. This includes data as clubs, games, players and player appearances from a number of national and international competitions and seasons.

Automate the data pipeline to **keep these assets up to date** and publicly available on well-known data catalogs for the data community's convenience.

:white_check_mark: [Kaggle](https://www.kaggle.com/davidcariboo/player-scores) :white_check_mark: [data.world](https://data.world/dcereijo/player-scores)

![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/diagram.png?raw=true)

### [data](data)
All project data assets are kept inside the `data` folder. This is a [DVC](https://dvc.org/) repository, therefore all files for the current revision can be pulled from remote storage with the `dvc pull` command.

> :information_source: Read access to the [DVC remote storage](https://dvc.org/doc/command-reference/remote#description) for this project is required to successfully run `dvc pull`. Contributors should feel free to grant themselves access by adding their AWS IAM user ARN to [this whitelist](https://github.com/dcaribou/transfermarkt-datasets/blob/6b6dd6572f582b2c40039913a65ba99d10fd1f44/infra/main.tf#L16). Have a look at [this PR](https://github.com/dcaribou/transfermarkt-datasets/pull/47/files) for an example.

`raw` data within this folder can be updated by running the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) with the `1_acquire.py` script.
```console
$ python 1_acquire.py local --asset all --season 2021
```

### [prep](prep)
Scripts for transforming scraped `raw` data into a cleaned, validated [data package](https://specs.frictionlessdata.io/) that can be used as the basis of further analysis in this project. You may run these scripts to produce the prepared dataset within `data/prep` using `2_prepare.py`.
```console
$ python 2_prepare.py local [--raw-files-location data/raw]
```
For reference on the types of assets produced by this script checkout published datasets linked above.

The preparation step uses `raw` data as input, hence raw files need to be available locally in order to run this step. You may pull raw assets by running `dvc pull` as mentioned earlier or by acquiring new and updated raw assets via `1_acquire.py` 

### [infra](infra)
Define all the necessary infrastructure for the project in the cloud with Terraform.

