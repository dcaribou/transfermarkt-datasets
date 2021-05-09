### Why did we build it?
Though football (soccer) datasets for match aggregates are widely available, structured, publicly available datasets at **player appearance level** for those matches, such as that contained in [player detailed performance page in Transfermarkt](https://www.transfermarkt.co.uk/sergio-aguero/leistungsdaten/spieler/26399), are more difficult to find. This dataset aims to present that data in an accessible, standard format.

### What does it contain?
The dataset is composed of multiple CSV files with information on leagues, games, clubs, players and appearances that is **automatically updated once a week**. Each file contains the attributes of the entity and the IDs that can be used to join them together.

For example, the `appearances.csv` file contains **one row per player appearance**, i.e. one row per player per game played. For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within this dataset, such as `player_id` and `game_id`.

### How did we build it?
The source code that maintains this dataset [is available in Github](https://github.com/dcaribou/player-scores). On a high level, the project uses [transfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to pull the data from [Transfermark website](https://www.transfermarkt.co.uk/) and a set of Python scripts to curate it and publish it here.

> **Idea:** Watch the [`player-scores` Github repository](https://github.com/dcaribou/player-scores) for updates on bugfixes and improvements on this dataset

### What is the status?
This dataset is a work in progress. The best way to find out the status of the missing pieces is to check the `Issues` section on [player-scores](https://github.com/dcaribou/player-scores/issues). Some **current limitations** that you should probably be aware of are
* Only a subset of all European domestic leagues are covered ([issue here](https://github.com/dcaribou/player-scores/issues/27))
* The first and last season in the dataset is the current season, i. e. 2020-2021 ([issue here](https://github.com/dcaribou/player-scores/issues/12))
