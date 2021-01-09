# :soccer: player-scores
Use data from [trasfermarkt-scraper](https://github.com/dcaribou/transfermarkt-scraper) to compute player's scores and forecast performance. Use forecasts to improve decision-making when creating line-ups for games such as [Fantasy Football](https://fantasy.premierleague.com/), [Biwenger](https://www.biwenger.com/), etc. 

This is an ongoing project, that aims to achieve this goal incrementally by

1. Building a clean, public dataset of player appearances' statistics
2. Training a machine learning model that uses historical data to forecast player's performance on their next game
3. Create a line-up analysis tool by displaying forecasts on a dashboard

## [data](data)
All project data assets are kept inside this folder. This is a [DVC](https://dvc.org/) repository, therefore all files from the current revision can be pulled from remote storage with `dvc pull`.

## [prep](prep)
Transform scraped data into a cleaned and validated dataset that can be published, and used as the base of further analysis.

## [viz](viz)
Some custom visualizations built on top of appearances data are referenced here for demonstrative purposes.
