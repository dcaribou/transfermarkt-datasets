#!/bin/bash

set -ex

PROJECT_HOME=/app/transfermarkt-datasets
BRANCH=$1
COMMAND=$2

shift 2

if ! [ -d $PROJECT_HOME ]; then
    echo "Setting up project dir"
    if ! [ -z ${BRANCH+x} ]; then
        git clone --branch $BRANCH https://github.com/dcaribou/transfermarkt-datasets.git
        cd $PROJECT_HOME
    else
        echo "BRANCH is required to bootstrap the environment"
        exit 1
    fi
fi

cd $PROJECT_HOME
if $COMMAND "$@" ; then
    echo "Job succeeded"
    exit 0
else
    echo "Job failed"
    exit 1
fi
