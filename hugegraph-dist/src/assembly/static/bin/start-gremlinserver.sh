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
GREMLINSERVER_CONFIG_TAG=gremlin-server
: ${GSRV_STARTUP_TIMEOUT_S:=20}
: ${GSRV_SHUTDOWN_TIMEOUT_S:=20}
# 这里的IP和prot应该要动态替换
: ${GSRV_IP:=127.0.0.1}
: ${GSRV_PORT:=8182}

: ${SLEEP_INTERVAL_S:=2}
VERBOSE=

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

echo "Forking GremlinServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/gremlin-server.sh conf/gremlin-server.yaml &
else
    "$BIN"/gremlin-server.sh conf/gremlin-server.yaml >/dev/null 2>&1 &
fi
wait_for_startup 'GremlinServer' $GSRV_IP $GSRV_PORT $GSRV_STARTUP_TIMEOUT_S || {
    echo "See $BIN/../logs/gremlin-server.log for GremlinServer log output."  >&2
}