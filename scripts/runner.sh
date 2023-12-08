#!/bin/bash

# entrypoint for running processes
# a process is a script in the scripts/acquiring or scripts/syncers directory
#
# usage: ./runner.sh <processes folder> <process name>

# check if at least 2 arguments are passed
if [ $# -lt 2 ]; then
    echo "Invalid number of arguments"
    exit 1
fi

PROCESS_NAME=$2
PROCESS_SCRIPTS_DIR=$1

echo "Running process: $PROCESS_NAME"

shift 2

# find process type (python or bash)
# it also implicitly checks if the process exists
#
# args: process name
function process_type() {
    if [ -f "$PROCESS_SCRIPTS_DIR/$1.py" ]; then
        echo "python"
        return 0
    elif [ -f "$PROCESS_SCRIPTS_DIR/$1.sh" ]; then
        echo "bash"
        return 0
    else
        echo "invalid"
        return 1
    fi
}

process_type=$(process_type $PROCESS_NAME)

# check if process is valid and exit if not
if [ $process_type = "invalid" ]; then
    echo "Invalid process"
    exit 1
fi

# if it is valud, run the process
# pass all remaining arguments to the process enclsing them in quotes
if [ $process_type == "python" ]; then
    python "$PROCESS_SCRIPTS_DIR/$PROCESS_NAME.py" $@
elif [ $process_type == "bash" ]; then
    bash "$PROCESS_SCRIPTS_DIR/$PROCESS_NAME.sh" $@
fi
