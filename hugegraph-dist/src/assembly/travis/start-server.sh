#!/bin/bash

set -ev

HOME_DIR=`pwd`
TRAVIS_DIR=`dirname $0`
BASE_DIR=$1
BACKEND=$2
BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/graphs/hugegraph.properties
REST_CONF=$BASE_DIR/conf/rest-server.properties
GREMLIN_CONF=$BASE_DIR/conf/gremlin-server.yaml

declare -A backend_serializer_map=(["memory"]="text" ["rocksdb"]="binary")

SERIALIZER=${backend_serializer_map[$BACKEND]}

# Set backend and serializer
sed -i "s/backend=.*/backend=$BACKEND/" $CONF
sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

$BIN/init-store.sh

AGENT_JAR=${HOME_DIR}/${TRAVIS_DIR}/jacocoagent.jar
$BIN/start-hugegraph.sh -j "-javaagent:${AGENT_JAR}=includes=*,port=36320,destfile=jacoco-it.exec,output=tcpserver" -v
