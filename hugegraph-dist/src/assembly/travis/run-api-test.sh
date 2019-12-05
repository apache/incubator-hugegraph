#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION

mvn package -DskipTests
$TRAVIS_DIR/start-server.sh $SERVER_DIR
mvn test -P api-test,$BACKEND || cat $SERVER_DIR/logs/hugegraph-server.log
$TRAVIS_DIR/build-report.sh
$TRAVIS_DIR/stop-server.sh
