"""
Generate datapackage.json for Kaggle Dataset
"""

from frictionless import describe_package

package = describe_package(source="appearances.csv", basepath="data/prep")
package.title = "Football players' statistics from Transfermarkt website"
package.keywords = [
  "football", "players", "stats", "statistics", "data",
  "soccer", "games", "matches"
]
package.id = "davidcariboo/player-scores"
package.image = "https://unsplash.com/photos/iOITF7T87kU"
package.description = """
### Context
Football (soccer) datasets containing historical match aggregates are widely available. However, structured, publicly available datasets on a **player level** for those games, such as the one contained in [Transfermarkt's player detailed performance page](https://www.transfermarkt.co.uk/diogo-jota/leistungsdatendetails/spieler/340950/saison/2020/verein/0/liga/0/wettbewerb/GB1/pos/0/trainer_id/0/plus/1), are more difficult to find. This dataset aims to present that data in an accessible, standard format.

### Content
The main resource within this dataset is the `appearances.csv` file, which contains **one row per player appearance** (one row per player per game played). Among other contextual data, on each appearance you will find a `date` and a `player_name`, as well as `goals` and `assists` metrics for that appearance. The contents of this file are automatically updated every week with the latest data from Transfermarkt website.<br/>
:information_source: Have a look at the version history for a record of the updates an improvements on this dataset.

### Acknowledgements
* Source data at [Transfermark website](https://www.transfermarkt.co.uk/)
* Data pipeline code for creating and keeping this dataset up to date is maintained at [player-scores](https://github.com/dcaribou/player-scores) github project
* Scraper code is published as an independent project [here](https://github.com/dcaribou/transfermarkt-scraper)

> Currently, this dataset contains data from `ES1 ` (Spanish first league) current season only, but there are more leagues, competitions and historical data to come. Checkout roadmap at [player-scores](https://github.com/dcaribou/player-scores)
"""
package.get_resource("appearances").name = "Appearances"

package.to_json("data/prep/dataset-metadata.json")
