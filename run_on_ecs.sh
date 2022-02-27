#!/bin/bash

BRANCH=fargate-tasks

git clone https://github.com/dcaribou/transfermarkt-datasets.git && \
# ln -s /app/dvc_cache transfermarkt-datasets/.dvc/cache && \ # TODO: this is for testing in local, remove before pushing
cd transfermarkt-datasets && \
git checkout $BRANCH &&
git pull

dvc pull
python 2_prepare.py && cp prep/stage/* data/prep

dvc commit && git push
