# :soccer: player-scores
Use data from [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to compute player's scores and forecast performance. Use forecasts to improve decision-making when creating line-ups for games such as [Fantasy Football](https://fantasy.premierleague.com/), [Biwenger](https://www.biwenger.com/), etc. 

This is an **ongoing project** that aims to achieve this goal incrementally by

- [x] Building a clean, public dataset of player appearances' statistics &#8594; [`dataset-improvements`](https://github.com/dcaribou/player-scores/issues?q=is%3Aissue+is%3Aopen+label%3A%22dataset+improvements%22)
- [ ] Training a machine learning model that uses historical data to forecast player's performance on their next game &#8594; [`machine-learning`](https://github.com/dcaribou/player-scores/issues?q=is%3Aissue+is%3Aopen+label%3A%22machine+learning%22)
- [ ] Create a line-up analysis tool by displaying forecasts on a dashboard &#8594; [`visualization`](https://github.com/dcaribou/player-scores/issues?q=is%3Aissue+is%3Aopen+label%3Avisualizations)

### [data](data)
All project data assets are kept inside the `data` folder. This is a [DVC](https://dvc.org/) repository, therefore all files for the current revision can be pulled from remote storage with `dvc pull`.

### [prep](prep)
Scripts for transforming scraped data into a cleaned, validated [data package](https://specs.frictionlessdata.io/) that can be used as the basis of further analysis in this project. For reference, checkout prepared datasets are published at
* [Kaggle Datasets](https://www.kaggle.com/davidcariboo/player-scores)
* [data.world Datasets](https://data.world/dcereijo/player-scores)

### [infra](infra)
Define all the necessary infrastructure for the project in the cloud.

