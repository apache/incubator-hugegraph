#!/bin/bash

set -ev

HOME_DIR=`pwd`
TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
BASE_DIR=hugegraph-$VERSION
BIN=$BASE_DIR/bin
CONF=$BASE_DIR/conf/hugegraph.properties

# PostgreSQL configurations
POSTGRESQL_DRIVER=org.postgresql.Driver
POSTGRESQL_URL=jdbc:postgresql://localhost:5432/
POSTGRESQL_USERNAME=postgres

declare -A backend_serializer_map=(["memory"]="text" ["cassandra"]="cassandra" \
                                   ["scylladb"]="scylladb" ["mysql"]="mysql" \
                                   ["hbase"]="hbase" ["rocksdb"]="binary" \
                                   ["postgresql"]="postgresql")

SERIALIZER=${backend_serializer_map[$BACKEND]}

# Set backend and serializer
sed -i "s/backend=.*/backend=$BACKEND/" $CONF
sed -i "s/serializer=.*/serializer=$SERIALIZER/" $CONF

# Set PostgreSQL configurations if needed
if [ "$BACKEND" == "postgresql" ]; then
    sed -i "s/#jdbc.driver=.*/jdbc.driver=$POSTGRESQL_DRIVER/" $CONF
    sed -i "s?#jdbc.url=.*?jdbc.url=$POSTGRESQL_URL?" $CONF
    sed -i "s/#jdbc.username=.*/jdbc.username=$POSTGRESQL_USERNAME/" $CONF
fi

# Append schema.sync_deletion=true to config file
echo "schema.sync_deletion=true" >> $CONF

AGENT_JAR=${HOME_DIR}/${TRAVIS_DIR}/jacocoagent.jar
$BIN/init-store.sh && $BIN/start-hugegraph.sh -j "-javaagent:${AGENT_JAR}=includes=*,port=36320,destfile=jacoco-it.exec,output=tcpserver" -v
