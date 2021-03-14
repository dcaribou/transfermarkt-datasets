"""
Generate datapackage.json for Kaggle Dataset
"""

from frictionless import describe_package

# full spec at https://specs.frictionlessdata.io//data-package/

package = describe_package(source="appearances.csv", basepath="data/prep")
package.title = "Football players' statistics from Transfermarkt website"
package.keywords = [
  "football", "players", "stats", "statistics", "data",
  "soccer", "games", "matches"
]
package.id = "davidcariboo/player-scores"
package.image = "https://images.unsplash.com/photo-1590669233095-90608d89c79c?ixlib=rb-1.2.1&ixid=MXwxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHw%3D&auto=format&fit=crop&w=2980&q=80"
package.licenses = [
  "CC0: Public Domain"
]
package.description = """
### Context
Football (soccer) datasets containing historical match aggregates are widely available. However, structured, publicly available datasets on a **player level** for those games, such as the one contained in [Transfermarkt's player detailed performance page](https://www.transfermarkt.co.uk/diogo-jota/leistungsdatendetails/spieler/340950/saison/2020/verein/0/liga/0/wettbewerb/GB1/pos/0/trainer_id/0/plus/1), are more difficult to find. This dataset aims to present that data in an accessible, standard format.

### Content
The main resource within this dataset is the `appearances.csv` file, which contains **one row per player appearance** (one row per player per game played). Among other contextual data, on each appearance you will find a `date` and a `player_name`, as well as `goals` and `assists` metrics for that appearance. The contents of this file are automatically updated every week with the latest data from Transfermarkt website.<br/>
:information_source: Have a look at the version history for a record of the updates an improvements on this dataset.

| Region  | League Code | Country     | Availablilty       | First Season |
|---------|-------------|-------------|--------------------|--------------|
| Europe  | ES1         | Spain       | :white_check_mark: | current      |
| Europe  | GB1         | England     | :white_check_mark: | current      |
| Europe  | L1          | Germany     | :white_check_mark: | current      |
| Europe  | NL1         | Netherlands | :white_check_mark: | current      |
| Europe  | DK1         | Denmark     | :white_check_mark: | current      |
| Europe  | TR1         | Turkey      | :white_check_mark: | current      |
| Europe  | GR1         | Greece      | :white_check_mark: | current      |
| Europe  | IT1         | Italy       | :white_check_mark: | current      |
| Europe  | BE1         | Belgium     | :white_check_mark: | current      |
| Europe  | PO1         | Portugal    | :white_check_mark: | current      |
| Europe  | FR1         | France      | :white_check_mark: | current      |
| Europe  | RU1         | Russia      | :white_check_mark: | current      |
| Europe  | UKR1        | Ukraine     | :white_check_mark: | current      |
| Europe  | SC1         | Scotland    | :white_check_mark: | current      |
| America | All         | All         | :x:                | -            |
| Asia    | All         | All         | :x:                | -            |
| Africa  | All         | All         | :x:                | -            |


### Acknowledgements
* Source data at [Transfermark website](https://www.transfermarkt.co.uk/)
* Data pipeline code for creating and keeping this dataset up to date is maintained at [player-scores](https://github.com/dcaribou/player-scores) github project
* Scraper code is published as an independent project [here](https://github.com/dcaribou/transfermarkt-scraper)

"""
appearances = package.get_resource("appearances")
appearances.name = "Appearances"
appearances.description = "One row per player and game (appearance)"
appearances.columns = [
  'competition',
  'round',
  'date',
  'player_position',
  'goals',
  'assists',
  'yellow_cards',
  'red_cards',
  'minutes_played',
  'player_club_name',
  'home_club_name',
  'home_club_id',
  'away_club_name',
  'away_club_id',
  'player_id',
  'player_name',
  'game_id',
  'appearance_id',
  'home_club_goals',
  'away_club_goals',
  'season'
 ]

package.to_json("data/prep/dataset-metadata.json")
