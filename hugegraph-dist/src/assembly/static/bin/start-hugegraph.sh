#!/bin/bash

if [ -z "$OPEN_MONITOR" ];then
  OPEN_MONITOR="false"
fi
if [ -z "$OPEN_SECURITY_CHECK" ];then
  OPEN_SECURITY_CHECK="true"
fi
if [ -z "$VERBOSE" ];then
  VERBOSE=""
fi
if [ -z "$GC_OPTION" ];then
  GC_OPTION=""
fi
if [ -z "$USER_OPTION" ];then
  USER_OPTION=""
fi
if [ -z "$SERVER_STARTUP_TIMEOUT_S" ];then
  SERVER_STARTUP_TIMEOUT_S=30
fi

if [ -z "$GRAPH_SPACE" ];then
  GRAPH_SPACE="DEFAULT"
fi
if [ -z "$SERVICE_ID" ];then
  SERVICE_ID="DEFAULT"
fi
if [ -z "$NODE_ID" ];then
  NODE_ID="node-1"
fi
if [ -z "$NODE_ROLE" ];then
  NODE_ROLE="worker"
fi
if [ -z "$META_SERVERS" ];then
  META_SERVERS="http://127.0.0.1:2379"
fi
if [ -z "$CLUSTER" ];then
  CLUSTER="hg"
fi
if [ -z "$PD_PEERS" ];then
  PD_PEERS="127.0.0.1:8686"
fi
if [ -z "$WITH_CA" ];then
  WITH_CA="false"
fi
if [ -z "$CA_FILE" ];then
  CA_FILE="conf/ca.perm"
fi
if [ -z "$CLIENT_CA" ];then
  CLIENT_CA="conf/client_ca.perm"
fi
if [ -z "$CLIENT_KEY" ];then
  CLIENT_KEY="conf/client.key"
fi

while getopts "g:m:s:j:G:S:N:R:M:E:W:C:A:K:P:v" arg; do
    case ${arg} in
        g) GC_OPTION="$OPTARG" ;;
        m) OPEN_MONITOR="$OPTARG" ;;
        s) OPEN_SECURITY_CHECK="$OPTARG" ;;
        j) USER_OPTION="$OPTARG" ;;
        G) GRAPH_SPACE="$OPTARG" ;;
        S) SERVICE_ID="$OPTARG" ;;
        N) NODE_ID="$OPTARG" ;;
        R) NODE_ROLE="$OPTARG" ;;
        M) META_SERVERS="$OPTARG" ;;
        E) CLUSTER="$OPTARG" ;;
        P) PD_PEERS="$OPTARG" ;;
        W) WITH_CA="$OPTARG" ;;
        C) CA_FILE="$OPTARG" ;;
        A) CLIENT_CA="$OPTARG" ;;
        K) CLIENT_KEY="$OPTARG" ;;
        v) VERBOSE="verbose" ;;
        ?) echo "USAGE: $0 [-g g1] [-m true|false] [-s true|false] [-j xxx] [-v] [-G graphspace] [-S serviceId] [-N nodeId] [-R nodeRole] [-M metaServer] [-E cluster] [-P pdAddress] [-W true|false] [-C caFile] [-A clientCa] [-K clientKey]" && exit 1 ;;
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

BIN=$(abs_path)
TOP="$(cd "$BIN"/../ && pwd)"
CONF="$TOP/conf"
LOGS="$TOP/logs"
PID_FILE="$BIN/pid"

if [ "$CA_FILE" = "" ];then
    CA_FILE="${CONF}/ca_file"
fi
if [ "$CLIENT_CA" = "" ];then
    CLIENT_CA="${CONF}/client_ca"
fi
if [ "$CLIENT_KEY" = "" ];then
    CLIENT_KEY="${CONF}/client_ca"
fi

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

"$BIN"/init-store.sh

echo "Starting HugeGraphServer..."

${BIN}/hugegraph-server.sh ${CONF}/gremlin-server.yaml ${CONF}/rest-server.properties \
${GRAPH_SPACE} ${SERVICE_ID} ${NODE_ID} ${NODE_ROLE} ${META_SERVERS} \
${CLUSTER} ${PD_PEERS} ${WITH_CA} ${CA_FILE} ${CLIENT_CA} ${CLIENT_KEY} \
${OPEN_SECURITY_CHECK} ${USER_OPTION} ${GC_OPTION} >>${LOGS}/hugegraph-server.log 2>&1 &

PID="$!"
# Write pid to file
echo "$PID" > "$PID_FILE"

trap 'kill $PID; exit' SIGHUP SIGINT SIGQUIT SIGTERM

wait_for_startup ${PID} 'HugeGraphServer' "$REST_SERVER_URL/graphs" ${SERVER_STARTUP_TIMEOUT_S} || {
    echo "See $LOGS/hugegraph-server.log for HugeGraphServer log output." >&2
    exit 1
}
disown

if [ "$OPEN_MONITOR" == "true" ]; then
    "$BIN"/start-monitor.sh
    if [ $? -ne 0 ]; then
        echo "Failed to open monitor, please start it manually"
    fi
    echo "An HugeGraphServer monitor task has been append to crontab"
fi
