#!/bin/bash

set -ev

VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
BASE_DIR=hugegraph-$VERSION
BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/hugegraph.properties

declare -A backend_serializer_map=(["memory"]="text" ["cassandra"]="cassandra" \
                                   ["scylladb"]="scylladb" ["mysql"]="mysql" \
                                   ["hbase"]="hbase" ["rocksdb"]="binary")

SERIALIZER=${backend_serializer_map[$BACKEND]}

sed -i "s/backend=.*/backend=$BACKEND/" $CONF
sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

$BIN/init-store.sh && $BIN/start-hugegraph.sh
