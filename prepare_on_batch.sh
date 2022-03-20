#!/bin/bash

set -ex

# pull data
dvc pull

# build prepared assets
python 2_prepare.py --raw-files-location data/raw --season 2021 # TODO: remove season filter
prep_status=$?
cp prep/stage/* data/prep

exit $prep_status
