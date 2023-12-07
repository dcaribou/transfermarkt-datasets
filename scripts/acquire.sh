#!/bin/bash

# entrypoint for running acquisition scripts
# usage: ./acquire.sh <acquirer name>

ACQUIRER_NAME=$1
ACQUIRING_SCRIPTS_DIR="scripts/acquiring"

echo "Running acquirer: $ACQUIRER_NAME"

shift 1

# check if acquirer name is provided
if [ $# -eq 0 ]; then
    echo "No acquirer name provided"
    exit 1
fi

# find acquirer type (python or bash)
# it also implicitly checks if the acquirer exists
# args: acquirer name
function acquirer_type() {
    if [ -f "$ACQUIRING_SCRIPTS_DIR/$1.py" ]; then
        echo "python"
        return 0
    elif [ -f "$ACQUIRING_SCRIPTS_DIR/$1.sh" ]; then
        echo "bash"
        return 0
    else
        echo "invalid"
        return 1
    fi
}

acquirer_type=$(acquirer_type $ACQUIRER_NAME)

# check if acquirer is valid and exit if not
if [ $acquirer_type = "invalid" ]; then
    echo "Invalid acquirer"
    exit 1
fi

# if it is valud, run the acquirer
if [ $acquirer_type == "python" ]; then
    python "$ACQUIRING_SCRIPTS_DIR/$ACQUIRER_NAME.py" $@
elif [ $acquirer_type == "bash" ]; then
    bash "$ACQUIRING_SCRIPTS_DIR/$ACQUIRER_NAME.sh" $@
fi
