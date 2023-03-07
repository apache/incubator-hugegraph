#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
OPEN_MONITOR="false"
OPEN_SECURITY_CHECK="true"
DAEMON="true"
#VERBOSE=""
GC_OPTION=""
USER_OPTION=""
SERVER_STARTUP_TIMEOUT_S=30

while getopts "d:g:m:s:j:t:v" arg; do
    case ${arg} in
        d) DAEMON="$OPTARG" ;;
        g) GC_OPTION="$OPTARG" ;;
        m) OPEN_MONITOR="$OPTARG" ;;
        s) OPEN_SECURITY_CHECK="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        t) SERVER_STARTUP_TIMEOUT_S="$OPTARG" ;;
        # TODO: should remove it in future (check the usage carefully)
        v) VERBOSE="verbose" ;;
        ?) echo "USAGE: $0 [-d true|false] [-g g1] [-m true|false] [-s true|false] [-j java_options]
                [-t timeout]" && exit 1 ;;
    esac
done

if [[ "$OPEN_MONITOR" != "true" && "$OPEN_MONITOR" != "false" ]]; then
    echo "USAGE: $0 [-d true|false] [-g g1] [-m true|false] [-s true|false] [-j java_options]"
    exit 1
fi

if [[ "$OPEN_SECURITY_CHECK" != "true" && "$OPEN_SECURITY_CHECK" != "false" ]]; then
    echo "USAGE: $0 [-d true|false] [-g g1] [-m true|false] [-s true|false] [-j java_options]"
    exit 1
fi

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$(cd -P "$(dirname "$SOURCE")" && pwd)"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    cd -P "$(dirname "$SOURCE")" && pwd
}

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
CONF="$TOP/conf"
LOGS="$TOP/logs"
PID_FILE="$BIN/pid"

. "$BIN"/util.sh

GREMLIN_SERVER_URL=$(read_property "$CONF/rest-server.properties" "gremlinserver.url")
if [ -z "$GREMLIN_SERVER_URL" ]; then
    GREMLIN_SERVER_URL="http://127.0.0.1:8182"
fi
REST_SERVER_URL=$(read_property "$CONF/rest-server.properties" "restserver.url")

check_port "$GREMLIN_SERVER_URL"
check_port "$REST_SERVER_URL"

if [ ! -d "$LOGS" ]; then
    mkdir -p "$LOGS"
fi

if [[ $DAEMON == "true" ]]; then
    echo "Starting HugeGraphServer in daemon mode..."
    "${BIN}"/hugegraph-server.sh "${CONF}"/gremlin-server.yaml "${CONF}"/rest-server.properties \
    "${OPEN_SECURITY_CHECK}" "${USER_OPTION}" "${GC_OPTION}" >>"${LOGS}"/hugegraph-server.log 2>&1 &
else
    echo "Starting HugeGraphServer in foreground mode..."
    "${BIN}"/hugegraph-server.sh "${CONF}"/gremlin-server.yaml "${CONF}"/rest-server.properties \
    "${OPEN_SECURITY_CHECK}" "${USER_OPTION}" "${GC_OPTION}" >>"${LOGS}"/hugegraph-server.log 2>&1
fi

PID="$!"
# Write pid to file
echo "$PID" > "$PID_FILE"

trap 'kill $PID; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup ${PID} 'HugeGraphServer' "$REST_SERVER_URL/graphs" "${SERVER_STARTUP_TIMEOUT_S}" || {
    echo "See $LOGS/hugegraph-server.log for HugeGraphServer log output." >&2
    if [[ $DAEMON == "true" ]]; then
        exit 1
    fi
}
disown

if [ "$OPEN_MONITOR" == "true" ]; then
    if ! "$BIN"/start-monitor.sh; then
        echo "Failed to open monitor, please start it manually"
    fi
    echo "An HugeGraphServer monitor task has been append to crontab"
fi
