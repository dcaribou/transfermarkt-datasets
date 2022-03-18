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

eval `ssh-agent -s`
ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')

dvc commit -f && git commit -m 'Acquired' && git push
