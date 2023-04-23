#!/bin/bash

abs_path() {
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

. "$BIN"/util.sh

PID_FILE=$BIN/pid
SERVER_SHUTDOWN_TIMEOUT_S=30

if [ ! -f ${PID_FILE} ]; then
    echo "The pid file $PID_FILE doesn't exist"
    exit 0
fi

PID=`cat $PID_FILE`
kill_process_and_wait "HugeGraphStoreServer" "$PID" "$SERVER_SHUTDOWN_TIMEOUT_S"

if [ $? -eq 0 ]; then
    rm "$PID_FILE"
fi

