# transfermarkt-datasets
Use data from [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to **build a clean, public football (soccer) dataset**. This includes data as clubs, games, players and player appearances from a number of national and international competitions and seasons.

Automate the data pipeline to **keep these assets up to date** and publicly available on well-known data catalogs for the data community's convenience.

:white_check_mark: [Kaggle](https://www.kaggle.com/davidcariboo/player-scores) :white_check_mark: [data.world](https://data.world/dcereijo/player-scores)

### [data](data)
All project data assets are kept inside the `data` folder. This is a [DVC](https://dvc.org/) repository, therefore all files for the current revision can be pulled from remote storage with the `dvc pull` command.

> :information_source: Read access to the [DVC remote storage](https://dvc.org/doc/command-reference/remote#description) for this project is required to successfully run `dvc pull`. Contributors should feel free to grant themselves access by adding their AWS IAM user ARN to [this whitelist](https://github.com/dcaribou/transfermarkt-datasets/blob/f5bda59b3a4fccf71fcef5a165591d441ab75e2d/infra/main.tf#L16). Have a look at [this PR](https://github.com/dcaribou/transfermarkt-datasets/pull/47/files) for an example.

### [prep](prep)
Scripts for transforming scraped data into a cleaned, validated [data package](https://specs.frictionlessdata.io/) that can be used as the basis of further analysis in this project. For reference, checkout prepared datasets are published at
* [Kaggle Datasets](https://www.kaggle.com/davidcariboo/player-scores)
* [data.world Datasets](https://data.world/dcereijo/player-scores)

### [infra](infra)
Define all the necessary infrastructure for the project in the cloud.

