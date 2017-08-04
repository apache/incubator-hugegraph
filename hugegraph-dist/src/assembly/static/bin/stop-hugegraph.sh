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
GSRV_CONFIG_TAG=cassandra-es
: ${CASSANDRA_SHUTDOWN_TIMEOUT_S:=60}
: ${HSRV_SHUTDOWN_TIMEOUT_S:=30}
: ${GSRV_SHUTDOWN_TIMEOUT_S:=30}
: ${SLEEP_INTERVAL_S:=2}
VERBOSE=
COMMAND=

JPS=
for maybejps in jps "${JAVA_HOME}/bin/jps"; do
    type "$maybejps" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        JPS="$maybejps"
        break
    fi
done

if [ -z "$JPS" ]; then
    echo "jps command not found.  Put the JDK's jps binary on the command path." >&2
    exit 1
fi

wait_for_shutdown() {
    local friendly_name="$1"
    local class_name="$2"
    local timeout_s="$3"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    while [ $now_s -le $stop_s ]; do
        status_class "$friendly_name" $class_name >/dev/null
        if [ $? -eq 1 ]; then
            # Class not found in the jps output.  Assume that it stopped.
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo "$friendly_name shutdown timeout exceeded ($timeout_s seconds)" >&2
    return 1
}

status_class() {
    local p=`$JPS -l | grep "$2" | awk '{print $1}'`
    if [ -n "$p" ]; then
        echo "$1 ($2) is running with pid $p"
        return 0
    else
        echo "$1 ($2) does not appear in the java process table"
        return 1
    fi
}

kill_class() {
    local pids=`$JPS -l | grep "$2" | awk '{print $1}' | xargs`

    for pid in ${pids[@]}
    do
        if [ -z "$pid" ]; then
            echo "$1 ($2) not found in the java process table"
            return
        fi
        echo "Killing $1 (pid $pid)..." >&2
        case "`uname`" in
            CYGWIN*) taskkill /F /PID "$pid" ;;
            *)       kill "$pid" ;;
        esac
    done
}

kill_class        'HugeGraphServer' com.baidu.hugegraph.dist.HugeGraphServer
wait_for_shutdown 'HugeGraphServer' com.baidu.hugegraph.dist.HugeGraphServer $HSRV_SHUTDOWN_TIMEOUT_S
kill_class        'HugeGremlinServer' com.baidu.hugegraph.dist.HugeGremlinServer
wait_for_shutdown 'HugeGremlinServer' com.baidu.hugegraph.dist.HugeGremlinServer $GSRV_SHUTDOWN_TIMEOUT_S
