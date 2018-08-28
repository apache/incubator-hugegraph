#!/bin/bash

OPEN_MONITOR="false"
VERBOSE=""
SERVER_STARTUP_TIMEOUT_S=30

while getopts "m:v" arg; do
    case ${arg} in
        m) OPEN_MONITOR="$OPTARG" ;;
        v) VERBOSE="verbose" ;;
        ?) echo "USAGE: $0 [-m true|false] [-v]" && exit 1 ;;
    esac
done

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
CONF=$TOP/conf
PID_FILE=$BIN/pid

. $BIN/util.sh

GREMLIN_SERVER_URL=`read_property "$CONF/rest-server.properties" "gremlinserver.url"`
REST_SERVER_URL=`read_property "$CONF/rest-server.properties" "restserver.url"`

check_port "$GREMLIN_SERVER_URL"
check_port "$REST_SERVER_URL"

echo "Starting HugeGraphServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/hugegraph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties &
else
    "$BIN"/hugegraph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties >/dev/null 2>&1 &
fi

PID="$!"
trap 'kill $PID; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup 'HugeGraphServer' "$REST_SERVER_URL/graphs" $SERVER_STARTUP_TIMEOUT_S || {
    echo "See $TOP/logs/hugegraph-server.log for HugeGraphServer log output." >&2
    exit 1
}
disown

# Write pid to file
echo "$PID" > $PID_FILE

if [ "$OPEN_MONITOR" == "true" ]; then
    $BIN/start-monitor.sh
    if [ $? -ne 0 ]; then
        echo "Failed to open monitor, please start it manually"
    fi
    echo "An HugeGraphServer monitor task has been append to crontab"
fi
