#!/bin/bash

set -ev

BACKEND=$1

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml

mvn package -DskipTests
cp $TRAVIS_DIR/graphs/hugegraph.properties $SERVER_DIR/conf/graphs/

$TRAVIS_DIR/start-server.sh $SERVER_DIR $BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)

# run api-test
mvn test -P api-test,$BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
$TRAVIS_DIR/build-report.sh $BACKEND
$TRAVIS_DIR/stop-server.sh
