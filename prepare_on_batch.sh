#!/bin/bash

set -ex

BRANCH=$1

eval `ssh-agent -s`
ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')

git config core.sshCommand "ssh -o StrictHostKeyChecking=no"

# pull code and data
git checkout -B $BRANCH
git branch --set-upstream-to=origin/$BRANCH $BRANCH
git pull
dvc pull

# build prepared assets
python 2_prepare.py --raw-files-location data/raw --season 2021 # TODO: remove season filter
prep_status=$?
cp prep/stage/* data/prep

# commit
if [ $prep_status == 0 ]; then
  dvc commit -f && git add data && git commit -m 'Prepared' && git push && dvc push
fi

