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

#read a property from a .properties file
function read_property(){
    # replace "." to "\."
    propertyName=`echo $2 | sed 's/\./\\\./g'`
    # file path
    fileName=$1;
    cat $fileName | sed -n -e "s/^[ ]*//g;/^#/d;s/^$propertyName=//p" | tail -1
}

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"

SERVER_URL=`read_property "$TOP/conf/rest-server.properties" "restserver.url"`

SERVER_STARTUP_TIMEOUT_S=30
SERVER_SHUTDOWN_TIMEOUT_S=30
SLEEP_INTERVAL_S=2

VERBOSE=

# check the port of rest server is occupied
function check_port() {

    local port=`echo $1 | awk -F':' '{print $3}'`
    lsof -i :$port >/dev/null

    if [ $? -eq 0 ]; then
        echo "The port "$port" has already used"
        exit 1
    fi
}

# wait_for_startup friendly_name host port timeout_s
function wait_for_startup() {

    local server_name="$1"
    local server_url="$2"
    local timeout_s="$3"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    local status

    echo -n "Connecting to $server_name ($server_url)"
    while [ $now_s -le $stop_s ]; do
        echo -n .
        status=`curl -o /dev/null -s -w %{http_code} $server_url`
        if [ $status -eq 200 ]; then
            echo "OK"
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo "timeout exceeded ($timeout_s seconds): could not connect to $server_url" >&2
    return 1
}

check_port $SERVER_URL

echo "Starting HugeGraphServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/graph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties &
else
    "$BIN"/graph-server.sh "$TOP"/conf/gremlin-server.yaml \
    "$TOP"/conf/rest-server.properties >/dev/null 2>&1 &
fi

pid="$!"
trap 'kill $pid; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup 'HugeGraphServer' "$SERVER_URL/graphs" $SERVER_STARTUP_TIMEOUT_S || {
    echo "See $TOP/logs/hugegraph-server.log for HugeGraphServer log output." >&2
}
disown
