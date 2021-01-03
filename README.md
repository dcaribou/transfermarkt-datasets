# :soccer: player-scores
Use data from the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to compute player's scores and forecast performance.

## [data](data)
All project data assets are generated inside this folder. This is a [DVC](https://dvc.org/) repository, therfore all files from the current version of the project can be pulled from their remote storage with `dvc pull`.

 * `raw/appearances.json` are the raw player appearances as the output of the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper). This file is required to run subsequent stages
 * `prep/appearances.csv` is a cleaned version of the previous one that is produced during the [prep](prep) stage

## [prep](prep)
Transform scraped data into a cleaned and validated dataset that can be used
as the base of the subsequent analysis.

## contrib

