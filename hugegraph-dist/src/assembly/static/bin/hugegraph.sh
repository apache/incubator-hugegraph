#!/bin/bash

# Returns the absolute path of this script regardless of symlinks
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
: ${CASSANDRA_STARTUP_TIMEOUT_S:=60}
: ${CASSANDRA_SHUTDOWN_TIMEOUT_S:=60}

: ${ELASTICSEARCH_STARTUP_TIMEOUT_S:=60}
: ${ELASTICSEARCH_SHUTDOWN_TIMEOUT_S:=60}
: ${ELASTICSEARCH_IP:=127.0.0.1}
: ${ELASTICSEARCH_PORT:=9300}

: ${GSRV_STARTUP_TIMEOUT_S:=20}
: ${GSRV_SHUTDOWN_TIMEOUT_S:=20}
: ${GSRV_IP:=127.0.0.1}
: ${GSRV_PORT:=8182}

: ${NOTEBOOK_STARTUP_TIMEOUT_S:=10}
: ${NOTEBOOK_SHUTDOWN_TIMEOUT_S:=10}
: ${NOTEBOOK_IP:=127.0.0.1}
: ${NOTEBOOK_PORT:=8080}

: ${SLEEP_INTERVAL_S:=2}
VERBOSE=
COMMAND=

# Locate the jps command.  Check $PATH, then check $JAVA_HOME/bin.
# This does not need to by cygpath'd.
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




start() {
    "$BIN"/start-cassandra.sh
    "$BIN"/start-gremlinserver.sh
    "$BIN"/start-hugeserver.sh
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

status() {
    status_class 'Gremlin-Server' org.apache.tinkerpop.gremlin.server.GremlinServer
    status_class Cassandra org.apache.cassandra.service.CassandraDaemon
    status_class Elasticsearch org.elasticsearch.bootstrap.Elasticsearch
}

clean() {
    echo -n "Are you sure you want to delete all stored data and logs? [y/N] " >&2
    read response
    if [ "$response" != "y" -a "$response" != "Y" ]; then
        echo "Response \"$response\" did not equal \"y\" or \"Y\".  Canceling clean operation." >&2
        return 0
    fi

    if cd "$BIN"/../db 2>/dev/null; then
        rm -rf cassandra es
        echo "Deleted data in `pwd`" >&2
        cd - >/dev/null
    else
        echo 'Data directory does not exist.' >&2
    fi

    if cd "$BIN"/../logs; then
        rm -f cassandra.log
        rm -f rexshugegraph.log
        echo "Deleted logs in `pwd`" >&2
        cd - >/dev/null
    fi
}

usage() {
    echo "Usage: $0 [options] {start|stop|status|clean}" >&2
    echo " start:  fork Cassandra, ES, and Gremlin-Server processes" >&2
    echo " stop:   kill running Cassandra, ES, and Gremlin-Server processes" >&2
    echo " status: print Cassandra, ES, and Gremlin-Server process status" >&2
    echo " clean:  permanently delete all graph data (run when stopped)" >&2
    echo "Options:" >&2
    echo " -v      enable logging to console in addition to logfiles" >&2
}

find_verb() {
    if [ "$1" = 'start' -o \
         "$1" = 'stop' -o \
         "$1" = 'clean' -o \
         "$1" = 'status' ]; then
        COMMAND="$1"
        return 0
    fi
    return 1
}

while [ 1 ]; do
    if find_verb ${!OPTIND}; then
        OPTIND=$(($OPTIND + 1))
    elif getopts 'c:v' option; then
        case $option in
        c) GSRV_CONFIG_TAG="${OPTARG}";;
        v) VERBOSE=yes;;
        *) usage; exit 1;;
        esac
    else
        break
    fi
done

if [ -n "$COMMAND" ]; then
    $COMMAND
else
    usage
    exit 1
fi
