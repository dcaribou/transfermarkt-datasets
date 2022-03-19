#!/bin/bash

set -ex

BRANCH=$1

# pull code and data
git checkout -B $BRANCH && \
dvc pull

# build prepared assets
python 2_prepare.py --raw-files-location data/raw --season 2021 # TODO: remove season filter
prep_status=$?
cp prep/stage/* data/prep

prep_status=0 # TODO: remove this line

# commit
if [ $prep_status == 0 ]; then
  eval `ssh-agent -s`
  ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')
  cat /root/.ssh/known_hosts # TODO: remove this line
  git log
  dvc commit -f && git add data && git commit -m 'Prepared' && git push -u origin $BRANCH && dvc push
fi

