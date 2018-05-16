#!/bin/bash

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"

. $BIN/util.sh

# Monitor HugeGraphServer every minute, if the server crashes then restart it.
# Modify the frequency according to actual needs carefully.
CRONTAB_JOB="*/1 * * * * $TOP/bin/monitor-hugegraph.sh"

crontab_append "$CRONTAB_JOB"
