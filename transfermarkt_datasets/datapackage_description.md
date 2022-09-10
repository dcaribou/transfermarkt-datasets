### TL;DR
> Clean, structured and **automatically updated** football data from [Transfermarkt](https://www.transfermarkt.co.uk/), including
> * `55,000+` games from many seasons on all major competitions
> * `400+` clubs from those competitions
> * `20,000+` players from those clubs
> * `300,000+` player market valuations historical records
> * `1,000,000+` player appearance records from all games

### What does it contain?
The dataset is composed of multiple CSV files with information on competitions, games, clubs, players and appearances that is automatically updated **once a week**. Each file contains the attributes of the entity and the IDs that can be used to join them together.

![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/resources/diagram.svg?raw=true)

For example, the `appearances` file contains **one row per player appearance**, i.e. one row per player per game played. For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within the dataset, such as `player_id` and `game_id`.
> **Idea:** Chekout the mate [data.world project Football Data from Transfermarkt](https://data.world/dcereijo/player-scores-demo) for examples of how to use the resources in this dataset to create different types of analysis.

### How did we build it?
The source code that maintains this dataset, as well as the data pipeline, [is available in Github](https://github.com/dcaribou/transfermarkt-datasets). On a high level, the project uses [transfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to pull the data from [Transfermark website](https://www.transfermarkt.co.uk/) and a set of Python scripts to curate it and publish it here.

> **Idea:** Watch the [`transfermarkt-datasets` Github repository](https://github.com/dcaribou/transfermarkt-datasets) for updates on improvements and bugfixes on this dataset

### What is the status?
This dataset is a live project subject to regular updates and new enhancements. The best way to find out about the different initiatives within this project and their status is to check the `Issues` section on [transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets/issues).

### Contact
The source of truth for the code and data for this project is Github and that is the best place to start a conversation. If you want to report a bug or propose a new feature, use the [`Issues`](https://github.com/dcaribou/transfermarkt-datasets/issues) or [`Discussions`](https://github.com/dcaribou/transfermarkt-datasets/discussions) sections.

> Bugfixes and enhancements contributing to this dataset are most welcome. If you want to contribute, the best way to start is by opening a new issue or picking an existing one from the [`Issues`](https://github.com/dcaribou/transfermarkt-datasets/issues) section in Github.
