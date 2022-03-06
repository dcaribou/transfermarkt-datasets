#!/bin/bash
set -ex

BRANCH=on-schedule-2022-03-07

# pull latest code
git checkout $BRANCH && git pull

# pull latest data
dvc pull

# acquire current season
# this will update the raw data partitions for the current seaon with the latest data
python 1_acquire.py --asset all --season 2021

dvc commit && git commit -m 'Acquired' && git push && dvc push
