#!/bin/bash

OPEN_MONITOR="true"
if [ $# -gt 1 ]; then
    echo "USAGE: $0 [true|false]"
    echo "The param indicates whether to add monitor, default is true"
    exit 1
elif [ $# -eq 1 ]; then
    if [[ $1 != "true" && $1 != "false" ]]; then
        echo "USAGE: $0 [true|false]"
        exit 1
    else
        OPEN_MONITOR=$1
    fi
fi

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

SERVER_URL=`read_property "$TOP/conf/rest-server.properties" "restserver.url"`
SERVER_STARTUP_TIMEOUT_S=30

VERBOSE=

check_port $SERVER_URL

echo "Starting HugeGraphServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/hugegraph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties &
else
    "$BIN"/hugegraph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties >/dev/null 2>&1 &
fi

pid="$!"
trap 'kill $pid; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup 'HugeGraphServer' "$SERVER_URL/graphs" $SERVER_STARTUP_TIMEOUT_S || {
    echo "See $TOP/logs/hugegraph-server.log for HugeGraphServer log output." >&2
    exit 1
}
disown

if [ "$OPEN_MONITOR" == "true" ]; then
    $BIN/start-monitor.sh
    if [ $? -ne 0 ]; then
        echo "Failed to open monitor, please start it manually"
    fi
    echo "An HugeGraphServer monitor task has been append to crontab"
fi
