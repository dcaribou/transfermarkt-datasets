⚠️ **The __Discussions__ tab in this website is not monitored. For getting in touch, please [use Github](https://github.com/dcaribou/transfermarkt-datasets/discussions)**

### TL;DR
> Clean, structured and **automatically updated** football data from [Transfermarkt](https://www.transfermarkt.co.uk/), including
> * `68,000+` games from many seasons on all major competitions
> * `400+` clubs from those competitions
> * `30,000+` players from those clubs
> * `450,000+` player market valuations historical records
> * `1,500,000+` player appearance records from all games
> 
> and more!

### What does it contain?
The dataset is composed of multiple CSV files with information on competitions, games, clubs, players and appearances that is automatically updated **once a week**. Each file contains the attributes of the entity and the IDs that can be used to join them together.

![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/resources/diagram.svg?raw=true)

For example, the `appearances` file contains **one row per player appearance**, i.e. one row per player per game played. For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within the dataset, such as `player_id` and `game_id`.

### How did we build it?
The source code that maintains this dataset, as well as the data pipeline, [is available in Github](https://github.com/dcaribou/transfermarkt-datasets). On a high level, the project uses [transfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to pull the data from [Transfermark website](https://www.transfermarkt.co.uk/) and a set of Python scripts and SQL to curate it and publish it here.

### What is the status?
This dataset is a live project subject to regular updates and new enhancements. The best way to find out about the different initiatives within this project and their status is to check the `Issues` section on [transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets/issues).

### Contact
The source of truth for the code and data for this project is Github and that is the best place to start a conversation, please checkout the community guidelines [here](https://github.com/dcaribou/transfermarkt-datasets/discussions/179).

> I kindly ask that you post your questions there and not in the __Discussion__ tab in this website, as that is not monitored.
