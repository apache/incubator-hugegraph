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

CASSANDRA_STARTUP_TIMEOUT_S=60
CASSANDRA_SHUTDOWN_TIMEOUT_S=60
SLEEP_INTERVAL_S=2

VERBOSE=

function wait_for_cassandra() {
    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $CASSANDRA_STARTUP_TIMEOUT_S ))

    echo -n 'Running `nodetool statusbinary`'
    while [ $now_s -le $stop_s ]; do
        echo -n .
        # The \r\n deletion bit is necessary for Cygwin compatibility
        statusbinary="`$BIN/nodetool statusbinary 2>/dev/null | tr -d '\n\r'`"
        if [ $? -eq 0 -a 'running' = "$statusbinary" ]; then
            echo 'OK'
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo " timeout exceeded ($CASSANDRA_STARTUP_TIMEOUT_S seconds)" >&2
    return 1
}

echo "Forking Cassandra..."
if [ -n "$VERBOSE" ]; then
    CASSANDRA_INCLUDE="$BIN"/cassandra.in.sh "$BIN"/cassandra || exit 1
else
    CASSANDRA_INCLUDE="$BIN"/cassandra.in.sh "$BIN"/cassandra >/dev/null 2>&1 || exit 1
fi
wait_for_cassandra || {
    echo "See $BIN/../logs/cassandra.log for Cassandra log output."    >&2
    return 1
}