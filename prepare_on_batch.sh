#!/bin/bash

set -ex

# git config core.sshCommand "ssh -o StrictHostKeyChecking=no" # TODO: decide on git this

# pull data
dvc pull

# build prepared assets
python 2_prepare.py --raw-files-location data/raw --season 2021 # TODO: remove season filter
prep_status=$?
cp prep/stage/* data/prep

# commit
if [ $prep_status == 0 ]; then
  dvc commit -f && git add data && git commit -m 'Prepared' && git push && dvc push
fi

