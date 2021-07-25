#!/bin/bash

# pull data from dvc remote storage
# this is to make sure all historical data is available locally in order to correctly build the assets
dvc pull

# acquire current season
# this will update the raw data partitions for the current seaon with the latest data
python acquire.py --asset all --season 2021

# build prepared assets
python prepare.py --raw-files-location data/raw
prep_status=$?
cp prep/stage/* data/prep

if [ $prep_status == 0 ]; then
  dvc commit -f
  dvc push
fi
