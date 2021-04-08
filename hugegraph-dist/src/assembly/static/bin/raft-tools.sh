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
    echo "usage: raft-tools.sh [options]"
    echo "options: "
    echo "  -l,--list_peers \${graph} \${group}                    list all peers' endpoints for graph, can be used on leader or follower node"
    echo "  -g,--get_leader \${graph} \${group}                    get the leader endpoint for graph, can be used on leader or follower node"
    echo "  -s,--set_leader \${graph} \${group} \${endpoint}        set the leader endpoint for graph, can be used on leader or follower node"
    echo "  -t,--transfer_leader \${graph} \${group} \${endpoint}   transfer leader to specified endpoint for graph, can be used on leader node"
    echo "  -a,--add_peer \${graph} \${group} \${endpoint}          add peer for graph, can be used on leader node"
    echo "  -r,--remove_peer \${graph} \${group} \${endpoint}       remove peer for graph, can be used on leader node"
    echo "  -h,--help                                            display help information"
}

GRAPH="hugegraph"
ENDPOINT=""

if [[ $# -lt 2 ]]; then
    print_usage
    exit 0
fi

function list_peers() {
    local graph=$1
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/list_peers

    curl ${url}
}

function get_leader() {
    local graph=$1
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/get_leader

    curl ${url}
}

function set_leader() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/set_leader?endpoint=${endpoint}

    curl -X POST ${url}
}

function transfer_leader() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/transfer_leader?endpoint=${endpoint}

    curl -X POST ${url}
}

function add_peer() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/add_peer?endpoint=${endpoint}

    curl -X POST ${url}
}

function remove_peer() {
    local graph=$1
    local endpoint=$2
    local rest_server_url=`read_property ${CONF_PATH}/rest-server.properties restserver.url`
    local url=${rest_server_url}/graphs/${graph}/raft/remove_peer?endpoint=${endpoint}

    curl -X POST ${url}
}

case $1 in
    # help
    --help|-h)
    print_usage
    ;;
    # list_peers
    --list_peers|-l)
    list_peers $2
    ;;
    # get_leader
    --get_leader|-g)
    get_leader $2
    ;;
    # set_leader
    --set_leader|-s)
    set_leader $2 $3
    ;;
    # transfer_leader
    --transfer_leader|-t)
    transfer_leader $2 $3
    ;;
    # add_peer
    --add_peer|-a)
    add_peer $2 $3
    ;;
    # remove_peer
    --remove_peer|-r)
    remove_peer $2 $3
    ;;
    *)
    print_usage
    exit 0
    ;;
esac
echo ""
