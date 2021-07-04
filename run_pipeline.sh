#!/bin/bash

# pull data from dvc remote storage
# this is to make sure all historical data is available locally in order to correctly build the assets
dvc pull 

# acquire current season
# this will update the raw data partitions for the current seaon with the latest data
python acquire.py --asset all --season 2020

# build prepared assets
(mkdir -p data/prep && cd prep && python prep.py --ignore-checks --raw-files-location ../data/raw && cp stage/* ../data/prep)
