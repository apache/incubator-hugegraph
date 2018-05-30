#!/bin/bash

CLOSE_MONITOR="true"

while getopts "m:" arg; do
    case ${arg} in
        m) CLOSE_MONITOR="$OPTARG" ;;
        ?) echo "USAGE: $0 [-m true|false]" && exit 1 ;;
    esac
done

if [[ "$CLOSE_MONITOR" != "true" && "$CLOSE_MONITOR" != "false" ]]; then
    echo "USAGE: $0 [-m true|false]"
    exit 1
fi

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

. $BIN/util.sh

PID_FILE=$BIN/pid
SERVER_SHUTDOWN_TIMEOUT_S=10

if [ "$CLOSE_MONITOR" == "true" ]; then
    $BIN/stop-monitor.sh
    if [ $? -ne 0 ]; then
        # TODO: If remove monitor failed, should continue kill process?
        echo "Failed to close monitor, please stop it manually via crontab -e"
    else
        echo "The HugeGraphServer monitor has been closed"
    fi
fi

if [ ! -f ${PID_FILE} ]; then
    echo "The pid file $PID_FILE doesn't exist"
    exit 1
fi

PID=`cat $PID_FILE`
kill_process_and_wait "HugeGraphServer" "$PID" "$SERVER_SHUTDOWN_TIMEOUT_S"

if [ $? -eq 0 ]; then
    rm "$PID_FILE"
fi