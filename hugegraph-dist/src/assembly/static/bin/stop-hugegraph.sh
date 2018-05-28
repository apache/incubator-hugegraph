#!/bin/bash

CLOSE_MONITOR="true"
if [ $# -gt 1 ]; then
    echo "USAGE: $0 [true|false]"
    echo "The param indicates whether to remove monitor, default is true"
    exit 1
elif [ $# -eq 1 ]; then
    if [[ $1 != "true" && $1 != "false" ]]; then
        echo "USAGE: $0 [true|false]"
        exit 1
    else
        CLOSE_MONITOR=$1
    fi
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

kill_process_and_wait "HugeGraphServer" com.baidu.hugegraph.dist.HugeGraphServer $SERVER_SHUTDOWN_TIMEOUT_S
