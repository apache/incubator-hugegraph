#!/bin/bash

export LANG=zh_CN.UTF-8
set -e

HOME_PATH=`dirname $0`
HOME_PATH=`cd ${HOME_PATH}/.. && pwd`
cd ${HOME_PATH}

BIN_PATH=${HOME_PATH}/bin
CONF_PATH=${HOME_PATH}/conf
LIB_PATH=${HOME_PATH}/lib
LOG_PATH=${HOME_PATH}/logs

. ${BIN_PATH}/util.sh

function print_usage() {
    echo "  usage: raft-tools.sh [options]"
    echo "  options: "
    echo "  -g,--get-leader \${graph}                     get the leader endpoint for graph"
    echo "  -s,--set-leader \${graph} \${endpoint}         set the leader endpoint for graph, can be used on leader or follower node"
    echo "  -t,--transfer-leader \${graph} \${endpoint}    transfer leader to specified endpoint for graph, can be used on leader node"
    echo "  -a,--add-peer \${graph} \${endpoint}        add peer for graph, can be used on leader node"
    echo "  -r,--remove-peer \${graph} \${endpoint}        remove peer for graph, can be used on leader node"
    echo "  -h,--help                                    display help information"
}

GRAPH="hugegraph"
ENDPOINT=""

if [[ $# -lt 2 ]]; then
    print_usage
    exit 0
fi

function get_leader() {
    local graph=$1
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/get-leader

    curl ${url}
}

function set_leader() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/set-leader?endpoint=${endpoint}

    curl -X POST ${url}
}

function transfer_leader() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/transfer-leader?endpoint=${endpoint}

    curl -X POST ${url}
}

function add_peer() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/add-peer?endpoint=${endpoint}

    curl -X POST ${url}
}

function remove_peer() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/remove-peer?endpoint=${endpoint}

    curl -X POST ${url}
}

while [[ $# -gt 0 ]]; do
    case $1 in
        # help
        --help|-h)
        print_usage
        shift
        ;;
        # get-leader
        --get-leader|-g)
        get_leader $2
        shift 2
        ;;
        # set-leader
        --set-leader|-s)
        set_leader $2 $3
        shift 3
        ;;
        # transfer-leader
        --transfer-leader|-t)
        transfer_leader $2 $3
        shift 3
        ;;
        # add-peer
        --add-peer|-r)
        add_peer $2 $3
        shift 3
        ;;
        # remove-peer
        --remove-peer|-r)
        remove_peer $2 $3
        shift 3
        ;;
        *)
        print_usage
        exit 0
        ;;
    esac
done
