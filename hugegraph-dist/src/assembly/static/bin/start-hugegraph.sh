#!/bin/bash

OPEN_MONITOR="true"
VERBOSE=""

function parse_args() {
    while getopts "m:v" arg; do
        case $arg in
            m) OPEN_MONITOR="$OPTARG" ;;
            v) VERBOSE="verbose" ;;
            ?) echo "USAGE: $0 [-m true|false] [-v]" && exit 1 ;;
        esac
    done
}

parse_args $@

if [[ "$OPEN_MONITOR" != "true" && "$OPEN_MONITOR" != "false" ]]; then
    echo "USAGE: $0 [-m true|false] [-v]"
    exit 1
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
