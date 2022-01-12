#!/bin/bash

set -ev

HOME_DIR=$(pwd)
TRAVIS_DIR=$(dirname $0)
BASE_DIR=$1
BACKEND=$2
JACOCO_PORT=$3

JACOCO_JAR=${HOME_DIR}/${TRAVIS_DIR}/jacocoagent.jar

BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/graphs/hugegraph.properties
REST_CONF=$BASE_DIR/conf/rest-server.properties
GREMLIN_CONF=$BASE_DIR/conf/gremlin-server.yaml

declare -A backend_serializer_map=(["memory"]="text" \
                                   ["cassandra"]="cassandra" \
                                   ["scylladb"]="scylladb" \
                                   ["mysql"]="mysql" \
                                   ["hbase"]="hbase" \
                                   ["rocksdb"]="binary" \
                                   ["postgresql"]="postgresql")

SERIALIZER=${backend_serializer_map[$BACKEND]}

# Set backend and serializer
sed -i "s/backend=.*/backend=$BACKEND/" $CONF
sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF

# Set PostgreSQL configurations if needed
if [ "$BACKEND" == "postgresql" ]; then
    sed -i '/org.postgresql.Driver/,+2 s/\#//g' $CONF
fi

# Set timeout for hbase
if [ "$BACKEND" == "hbase" ]; then
    sed -i '$arestserver.request_timeout=200' $REST_CONF
    sed -i '$agremlinserver.timeout=200' $REST_CONF
    sed -i 's/scriptEvaluationTimeout.*/scriptEvaluationTimeout: 200000/' $GREMLIN_CONF
fi

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

JACOCO_OPTION=""
if [ -n "$JACOCO_PORT" ]; then
    JACOCO_OPTION="-javaagent:${JACOCO_JAR}=includes=*,port=${JACOCO_PORT},destfile=jacoco-it.exec,output=tcpserver"
fi

echo -e "pa" | $BIN/init-store.sh && $BIN/start-hugegraph.sh -j "$JACOCO_OPTION" -t 60 -v
