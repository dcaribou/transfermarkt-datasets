⚠️ **The __Discussions__ tab in this website is not monitored. For getting in touch, please [use Github](https://github.com/dcaribou/transfermarkt-datasets/discussions)**

### TL;DR
> Clean, structured and **automatically updated** football data from [Transfermarkt](https://www.transfermarkt.co.uk/), including
> * `80,000+` games from many seasons on all major competitions
> * `400+` clubs from those competitions
> * `37,000+` players from those clubs
> * `520,000+` player market valuations historical records
> * `1,800,000+` player appearance records from all games
> * `99,000+` transfer records
> * `1,100,000+` game events (goals, cards, substitutions)
> * `100+` countries and national teams with FIFA rankings
>
> and more!

### What does it contain?
The dataset is composed of multiple CSV files that are automatically updated **once a week**. Each file contains the attributes of the entity and the IDs that can be used to join them together.

| File | Description |
|------|-------------|
| `competitions` | Leagues and tournaments tracked in the dataset |
| `games` | One row per game, with scores, attendance and referee |
| `clubs` | Club details including squad size and stadium |
| `players` | Player profiles, positions and market values |
| `player_valuations` | Historical market value records per player |
| `appearances` | One row per player per game, with goals, assists and cards |
| `game_events` | In-game events: goals, cards, substitutions |
| `game_lineups` | Starting and bench lineups per game |
| `club_games` | Game-level stats from each club's perspective |
| `transfers` | Player transfers between clubs with fees |
| `countries` | Countries with confederation and aggregate stats |
| `national_teams` | National teams with FIFA rankings and squad details |

![diagram](https://github.com/dcaribou/transfermarkt-datasets/blob/master/resources/diagram.svg?raw=true)

### How did we build it?
The source code that maintains this dataset, as well as the data pipeline, [is available in Github](https://github.com/dcaribou/transfermarkt-datasets). On a high level, the project uses [transfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to pull the data from [Transfermarkt website](https://www.transfermarkt.co.uk/) and a set of Python scripts and SQL to curate it and publish it here.

### What is the status?
This dataset is a live project subject to regular updates and new enhancements. The best way to find out about the different initiatives within this project and their status is to check the `Issues` section on [transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets/issues).

### Contact
The source of truth for the code and data for this project is Github and that is the best place to start a conversation, please checkout the community guidelines [here](https://github.com/dcaribou/transfermarkt-datasets/discussions/179).

> I kindly ask that you post your questions there and not in the __Discussion__ tab in this website, as that is not monitored.
