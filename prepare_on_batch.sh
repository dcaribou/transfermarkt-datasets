#!/bin/bash
set -ex

BRANCH=on-schedule-2022-03-07

# pull code and data
git checkout -B $BRANCH && \
dvc pull

# build prepared assets
python 2_prepare.py --raw-files-location data/raw
prep_status=$?
cp prep/stage/* data/prep

prep_status=0 # TODO: remove this line

# commit
if [ $prep_status == 0 ]; then
  ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')
  dvc commit -f && git commit -m 'Prepared' && git push
fi

