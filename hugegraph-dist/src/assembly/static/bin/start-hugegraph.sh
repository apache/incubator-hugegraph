#!/bin/bash

OPEN_MONITOR="false"
OPEN_SECURITY_CHECK="true"
VERBOSE=""
GC_OPTION=""
USER_OPTION=""
SERVER_STARTUP_TIMEOUT_S=30

while getopts "g:m:s:j:v" arg; do
    case ${arg} in
        g) GC_OPTION="$OPTARG" ;;
        m) OPEN_MONITOR="$OPTARG" ;;
        s) OPEN_SECURITY_CHECK="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        v) VERBOSE="verbose" ;;
        ?) echo "USAGE: $0 [-g g1] [-m true|false] [-s true|false] [-j xxx] [-v]" && exit 1 ;;
    esac
done

if [[ "$OPEN_MONITOR" != "true" && "$OPEN_MONITOR" != "false" ]]; then
    echo "USAGE: $0 [-g g1] [-m true|false] [-s true|false] [-j xxx] [-v]"
    exit 1
fi

if [[ "$OPEN_SECURITY_CHECK" != "true" && "$OPEN_SECURITY_CHECK" != "false" ]]; then
    echo "USAGE: $0 [-g g1] [-m true|false] [-s true|false] [-j xxx] [-v]"
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
CONF="$TOP/conf"
LOGS="$TOP/logs"
PID_FILE="$BIN/pid"

. $BIN/util.sh

GREMLIN_SERVER_URL=`read_property "$CONF/rest-server.properties" "gremlinserver.url"`
if [ -z "$GREMLIN_SERVER_URL" ]; then
    GREMLIN_SERVER_URL="http://127.0.0.1:8182"
fi
REST_SERVER_URL=`read_property "$CONF/rest-server.properties" "restserver.url"`

check_port "$GREMLIN_SERVER_URL"
check_port "$REST_SERVER_URL"

echo "Starting HugeGraphServer..."

"$BIN"/hugegraph-server.sh "$CONF"/gremlin-server.yaml "$CONF"/rest-server.properties \
"$OPEN_SECURITY_CHECK" "$USER_OPTION" "$GC_OPTION" >>"$LOGS/hugegraph-server.log" 2>&1 &

PID="$!"
# Write pid to file
echo "$PID" > $PID_FILE

trap 'kill $PID; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup ${PID} 'HugeGraphServer' "$REST_SERVER_URL/graphs" ${SERVER_STARTUP_TIMEOUT_S} || {
    echo "See $LOGS/hugegraph-server.log for HugeGraphServer log output." >&2
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
