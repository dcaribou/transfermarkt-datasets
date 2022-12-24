#!/bin/bash

set -ex

PROJECT_HOME=/app/transfermarkt-datasets
BRANCH=$1
COMMAND=$2

shift 2

if ! [ -d $PROJECT_HOME ]; then
    echo "Setting up project dir"
    if ! [ -z ${BRANCH+x} ]; then
        eval `ssh-agent -s`
        ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')
        git clone --branch $BRANCH git@github.com:dcaribou/transfermarkt-datasets.git
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
