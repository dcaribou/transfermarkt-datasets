# :soccer: player-scores
Use data from the [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to compute player's scores and forecast performance.

## [data](data)
All project data assets are generated inside this folder. This is a [DVC](https://dvc.org/) repository, therfore all files for the currrent version of the project can be pulled from a their remote storage with
```bash
dvc pull
```
`data/appearances.json` is the only file required to run subsequent stages.

## [prep](prep)
Transform scraped data into a cleaned and validated dataset that can be used
as the base of the subsequent analysis.
