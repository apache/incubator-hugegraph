#!/bin/bash

set -ev

BACKEND=$1

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION
RAFT1_DIR=hugegraph-raft1
RAFT2_DIR=hugegraph-raft2
RAFT3_DIR=hugegraph-raft3
CONF=$SERVER_DIR/conf/graphs/hugegraph.properties
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml

JACOCO_PORT=36320
RAFT_TOOLS=$RAFT1_DIR/bin/raft-tools.sh
RAFT_LEADER="127.0.0.1:8281"

mvn package -DskipTests

# mkdir for each raft-server
cp -r $SERVER_DIR $RAFT1_DIR
cp -r $SERVER_DIR $RAFT2_DIR
cp -r $SERVER_DIR $RAFT3_DIR

# config raft-server (must keep '/.')
cp -rf $TRAVIS_DIR/conf-raft1/. $RAFT1_DIR/conf/
cp -rf $TRAVIS_DIR/conf-raft2/. $RAFT2_DIR/conf/
cp -rf $TRAVIS_DIR/conf-raft3/. $RAFT3_DIR/conf/

# start server
$TRAVIS_DIR/start-server.sh $RAFT1_DIR $BACKEND $JACOCO_PORT || (cat $RAFT1_DIR/logs/hugegraph-server.log && exit 1) &
$TRAVIS_DIR/start-server.sh $RAFT2_DIR $BACKEND || (cat $RAFT2_DIR/logs/hugegraph-server.log && exit 1) &
$TRAVIS_DIR/start-server.sh $RAFT3_DIR $BACKEND || (cat $RAFT3_DIR/logs/hugegraph-server.log && exit 1)

$RAFT_TOOLS --set-leader "hugegraph" "$RAFT_LEADER"

# run api-test
mvn test -P api-test,$BACKEND || (cat $RAFT1_DIR/logs/hugegraph-server.log && exit 1)

$TRAVIS_DIR/build-report.sh $BACKEND $JACOCO_PORT

# stop server
$TRAVIS_DIR/stop-server.sh $RAFT1_DIR
$TRAVIS_DIR/stop-server.sh $RAFT2_DIR
$TRAVIS_DIR/stop-server.sh $RAFT3_DIR
