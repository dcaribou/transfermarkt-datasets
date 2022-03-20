#!/bin/bash

set -ex

PROJECT_HOME=/app/transfermarkt-datasets
BRANCH=$1

if ! [ -d $PROJECT_HOME ]; then
    echo "Setting up project dir"
    if ! [ -z ${BRANCH+x} ]; then
        eval `ssh-agent -s`
        ssh-add - <<< $(aws --region eu-west-1 secretsmanager get-secret-value --secret-id /ssh/transfermarkt-datasets/deploy-keys | jq -r '.SecretString')
        git clone --branch $BRANCH git@github.com:dcaribou/transfermarkt-datasets.git
    else
        echo "BRANCH is required to bootstrap the environment"
        exit 1
    fi
fi
shift

(cd $PROJECT_HOME ; bash "$@")
