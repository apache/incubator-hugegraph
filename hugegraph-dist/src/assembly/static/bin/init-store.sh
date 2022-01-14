#!/bin/bash

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

while getopts "G:S:N:R:M:E:W:C:A:K:P:v" arg; do
    case ${arg} in
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
        ?) echo "USAGE: $0 [-G graphspace] [-S serviceId] [-N nodeId] [-R nodeRole] [-M metaServer] [-E cluster] [-P pdAddress] [-W true|false] [-C caFile] [-A clientCa] [-K clientKey]" && exit 1 ;;
    esac
done

abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [[ -h "$SOURCE" ]]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ ${SOURCE} != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

BIN=`abs_path`
TOP="$(cd ${BIN}/../ && pwd)"
CONF="$TOP/conf"
LIB="$TOP/lib"
PLUGINS="$TOP/plugins"

. ${BIN}/util.sh

ensure_path_writable ${PLUGINS}

if [[ -n "$JAVA_HOME" ]]; then
    JAVA="$JAVA_HOME"/bin/java
    EXT="$JAVA_HOME/jre/lib/ext:$LIB:$PLUGINS"
else
    JAVA=java
    EXT="$LIB:$PLUGINS"
fi

cd ${TOP}

echo "Initializing HugeGraph Store..."

${JAVA} -cp ${LIB}/hugegraph-dist-*.jar -Djava.ext.dirs=${LIB}:${PLUGINS} \
    com.baidu.hugegraph.cmd.InitStore ${CONF}/rest-server.properties \
    ${GRAPH_SPACE} ${SERVICE_ID} ${NODE_ID} ${NODE_ROLE} ${META_SERVERS} \
    ${CLUSTER} ${PD_PEERS} ${WITH_CA} ${CA_FILE} ${CLIENT_CA} ${CLIENT_KEY}

echo "Initialization finished."
