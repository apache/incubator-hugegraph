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
HUGESERVER_CONFIG_TAG=huge-server
: ${HSRV_STARTUP_TIMEOUT_S:=20}
: ${HSRV_SHUTDOWN_TIMEOUT_S:=20}
: ${HSRV_IP:=127.0.0.1}
: ${HSRV_PORT:=8080}

: ${SLEEP_INTERVAL_S:=2}
VERBOSE="V"

# wait_for_startup friendly_name host port timeout_s
wait_for_startup() {
    local friendly_name="$1"
    local host="$2"
    local port="$3"
    local timeout_s="$4"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))
    local status=

    echo -n "Connecting to $friendly_name ($host:$port)"
    while [ $now_s -le $stop_s ]; do
        echo -n .
        $BIN/checksocket.sh $host $port >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "OK (connected to $host:$port)"
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo "timeout exceeded ($timeout_s seconds): could not connect to $host:$port" >&2
    return 1
}

echo "Forking HugeServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/huge-server.sh conf/huge-server.properties &
else
    "$BIN"/huge-server.sh conf/huge-server.properties >/dev/null 2>&1 &
fi
wait_for_startup 'HugeServer' $HSRV_IP $HSRV_PORT $HSRV_STARTUP_TIMEOUT_S || {
    echo "See $BIN/../logs/huge-server.log for HugeServer log output."  >&2
}
disown
