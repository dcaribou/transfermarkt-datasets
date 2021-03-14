# Data Prep

Loads the output of a [transfermark-webscrapper](https://github.com/dcaribou/transfermarkt-scraper) run, and a applies a series of transformations to produce a file that is validated and more friendly for perfoming analyisis. Some of these transformations are

* Creating handy ID columns
* Renaming fileds to comply with naming convention
* Parsing raw values into their own columns

## Load
Input to the data prep process is excepted to be the output of the [transfermark-webscrapper](https://github.com/dcaribou/transfermarkt-scraper). I. e., a file with JSON lines with one line per player containing all player appearances on a game up to the date the scraper was run.

Later on we will use [papermill](https://papermill.readthedocs.io/en/latest/usage-parameterize.html) to run the notebook from a dvc pipeline. The next cells define [notebook-wide parameters](https://papermill.readthedocs.io/en/latest/usage-parameterize.html) than can be overridded via papermill arguments.

## Prep
The prep phase applies a series of transformations on the raw data frame that we loaded above

### Flatten
Firstly, we need to explode the data frame to have one row per player appearance, rather than one row per player

Rename
Modify the names of the input columns to make them consistent with a naming convention

### Update
- [x] Convert `goals`, `assists`, `own_goals` and `date` to the appropriate types
- [x] Revamp `yellow_cards` and `red_cards`. `second_yellows` column is not needed
- [ ] Club name prettifying. _FC Watford_ instead of _fc-watford_
- [ ] Player name prettifying. _Adam Masina_ instead of _adam-masina_
- [x] Use longer names for `position` instead of the chryptic 'LB', etc. (use 'filter by position' [here](https://www.transfermarkt.co.uk/statistik/topscorer) to get the mappings)

### Create
- [x] Add surrogate keys `game_id`, `player_id`, `appearance_id`, `home_club_id`, `away_club_id`
- [x] Split `result` into `home_club_goals` and `away_club_goals`
- [x] Approximate appearance `season`

### Filter
* Only season 2020 is complete on the current file, so we remove the rest
* To reduce the scope of this version of the data prep scritp, select only appearances from domestic competitions

## Validate
Validate that the output dataframe contains consistent data. Two types of checks are performed.

### Value checks
- [x] Fields `red_cards`, `yellow_cards`, `own_goals`, `assists`, `goals` and `minutes_played` contain values within an expected range
- [x] Rows are unique on `player_id` + `date`
- [ ] `position` field is either one of the long form player positions from Transfermarkt

### Completeness checks
- [x] Number of teams per domestic competition must be exactly 20
- [ ] Each club must play 38 games per season on the domestic competition
- [ ] On each match, both clubs should have at least 11 appearances
- [ ] Similarly, each club must have at least 11 appearances per game
