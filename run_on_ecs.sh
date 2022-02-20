#!/bin/bash

BRANCH=fargate-ecs

git clone git@github.com:dcaribou/transfermarkt-datasets.git && \
cd transfermarkt-datasets && \
git checkout $BRANCH &&
git pull

dvc pull
python 2_prepare.py && cp prep/stage/* data/prep

dvc commit && git push
